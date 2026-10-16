import os
import sys
import time
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# Forzar codificación UTF-8 en la consola de Windows para evitar errores con emojis
if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

db_user = os.getenv("MONGO_USER")
db_pass = os.getenv("MONGO_PASS")
db_cluster = os.getenv("MONGO_CLUSTER")

if not db_user or not db_pass or not db_cluster:
    print("❌ Error: Faltan credenciales en el archivo .env")
    sys.exit(1)

MONGO_URI = f"mongodb+srv://{db_user}:{db_pass}@{db_cluster}/?retryWrites=true&w=majority"

# Conexión a MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client["Monitor_P2P_Bolivia"]
    client.admin.command('ping')
    print("✅ Conexión exitosa a MongoDB.")
except Exception as e:
    print(f"❌ Error crítico de conexión a MongoDB: {e}")
    sys.exit(1)

# Definición de los exchanges que vamos a monitorear
EXCHANGES_CONFIG = [
    {
        "nombre": "bybit",
        "coleccion_precios": db["BBT_PRICE"],
        "coleccion_ordenes": db["1EXECUTED_ORDERS"]
    },
    {
        "nombre": "binance",
        "coleccion_precios": db["BNC_PRICE"],
        "coleccion_ordenes": db["1EXECUTED_ORDERS"]
    },
    {
        "nombre": "bingx",
        "coleccion_precios": db["BGX_PRICE"],
        "coleccion_ordenes": db["1EXECUTED_ORDERS"]
    },
    {
        "nombre": "mexc",
        "coleccion_precios": db["MXC_PRICE"],
        "coleccion_ordenes": db["1EXECUTED_ORDERS"]
    }
]

def extraer_comerciantes(lista_merchants):
    """Mapea los comerciantes usando su nickName y maneja tipos de datos mixtos (strings, floats o BSON Doubles)."""
    diccionario = {}
    if not lista_merchants:
        return diccionario
    for m in lista_merchants:
        nick = m.get("nickName")
        tradable_qty = m.get("tradableQuantity")
        
        if nick and tradable_qty is not None:
            try:
                qty_float = float(str(tradable_qty))
                price_val = m.get("price")
                price_float = float(str(price_val)) if price_val is not None else 0.0
                
                diccionario[nick] = {
                    "tradableQuantity": round(qty_float, 2),
                    "price": round(price_float, 3)
                }
            except (ValueError, TypeError):
                continue
                
    return diccionario

def comparar_seccion(anterior_dict, actual_dict, tipo_seccion, exchange_val):
    """
    Compara solo 'tradableQuantity'.
    - Si se reduce: registra VENTA_USDT o COMPRA_USDT.
    - Si desaparece: 
        * merchant_buy -> 'venta_liquidado/apagado'
        * merchant_sell -> 'compra_liquidado/apagado'
    """
    eventos = []
    
    # Definir las acciones según la sección
    if tipo_seccion == "merchant_buy":
        accion_normal = "VENTA_USDT"
        accion_liquidado = "venta_liquidado/apagado"
    else:
        accion_normal = "COMPRA_USDT"
        accion_liquidado = "compra_liquidado/apagado"
    
    for nick, datos_ant in anterior_dict.items():
        qty_ant = datos_ant["tradableQuantity"]
        
        if nick in actual_dict:
            qty_act = actual_dict[nick]["tradableQuantity"]
            
            # Solo nos interesa si la cantidad SE REDUCE
            if qty_act < qty_ant:
                diferencia = round(qty_ant - qty_act, 2)
                eventos.append({
                    "nickName": nick,
                    "accion": accion_normal,
                    "cantidad_transaccionada": diferencia,
                    "cantidad_anterior": round(qty_ant, 2),
                    "cantidad_actual": round(qty_act, 2),
                    "precio": datos_ant.get("price"),
                    "exchange": exchange_val
                })
        else:
            # El anuncio desapareció (liquidado / apagado)
            diferencia = round(qty_ant, 2)
            eventos.append({
                "nickName": nick,
                "accion": accion_liquidado,
                "cantidad_transaccionada": diferencia,
                "cantidad_anterior": round(qty_ant, 2),
                "cantidad_actual": 0.0,
                "precio": datos_ant.get("price"),
                "exchange": exchange_val
            })

    return eventos

def procesar_exchange(cfg):
    """Procesa la lógica de extracción y comparación para un exchange específico."""
    nombre_exchange = cfg["nombre"]
    col_price = cfg["coleccion_precios"]
    col_orders = cfg["coleccion_ordenes"]
    
    try:
        registros = list(col_price.find().sort("timestamp", -1).limit(2))
        
        if len(registros) < 2:
            return  # No hay suficientes registros aún para este exchange

        registro_actual = registros[0]
        registro_anterior = registros[1]

        id_actual = registro_actual["_id"]
        id_anterior = registro_anterior["_id"]

        # Evitar duplicados por exchange y par de IDs ya analizados
        existe_registro = col_orders.find_one({
            "exchange": nombre_exchange,
            "id_registro_actual": id_actual,
            "id_registro_anterior": id_anterior
        })

        if existe_registro:
            return

        # Extraer comerciantes de ambos registros
        buy_anterior = extraer_comerciantes(registro_anterior.get("merchant_buy", []))
        buy_actual = extraer_comerciantes(registro_actual.get("merchant_buy", []))
        
        sell_anterior = extraer_comerciantes(registro_anterior.get("merchant_sell", []))
        sell_actual = extraer_comerciantes(registro_actual.get("merchant_sell", []))

        # 🚀 OPTIMIZACIÓN: Si no hay cambios en absoluto entre las dos listas, salimos sin guardar nada
        if buy_anterior == buy_actual and sell_anterior == sell_actual:
            return

        # Procesar eventos si los diccionarios difieren
        eventos_buy = comparar_seccion(buy_anterior, buy_actual, "merchant_buy", nombre_exchange)
        eventos_sell = comparar_seccion(sell_anterior, sell_actual, "merchant_sell", nombre_exchange)

        # Si existen eventos reales (reducciones o liquidaciones), guardamos en la colección de órdenes
        if eventos_buy or eventos_sell:
            resultado_analisis = {
                "exchange": nombre_exchange,
                "timestamp_analisis": datetime.utcnow(),
                "id_registro_actual": id_actual,
                "id_registro_anterior": id_anterior,
                "merchant_buy_eventos": eventos_buy if eventos_buy else None,
                "merchant_sell_eventos": eventos_sell if eventos_sell else None
            }

            col_orders.insert_one(resultado_analisis)
            print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] 🔍 [{nombre_exchange.upper()}] Reducciones y liquidaciones detectadas y guardadas.")

    except Exception as e:
        print(f"❌ Error procesando el exchange {nombre_exchange}: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando monitor P2P Multi-Exchange (Bybit, Binance, BingX, MEXC)... Ejecutándose cada 10 segundos...")
    try:
        while True:
            for conf in EXCHANGES_CONFIG:
                procesar_exchange(conf)
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n🛑 Monitor detenido por el usuario.")
        client.close()
        sys.exit(0)