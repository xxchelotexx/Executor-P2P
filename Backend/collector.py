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

# --- CONFIGURACIÓN DE ORIGEN (Lectura de precios) ---
db_user = os.getenv("MONGO_USER")
db_pass = os.getenv("MONGO_PASS")
db_cluster = os.getenv("MONGO_CLUSTER")
db_name_source = os.getenv("MONGO_DB_NAME", "Monitor_P2P_Bolivia")

# --- CONFIGURACIÓN DE DESTINO (Escritura de órdenes) ---
db_user2 = os.getenv("MONGO_USER2")
db_pass2 = os.getenv("MONGO_PASS2")
db_cluster2 = os.getenv("MONGO_CLUSTER2")
db_name_target = os.getenv("MONGO_DB_NAME2", "Monitor_P2P_Bolivia")

# Validar credenciales de origen
if not db_user or not db_pass or not db_cluster:
    print("❌ Error: Faltan credenciales de ORIGEN en el archivo .env (MONGO_USER, MONGO_PASS, MONGO_CLUSTER)")
    sys.exit(1)

# Validar credenciales de destino
if not db_user2 or not db_pass2 or not db_cluster2:
    print("❌ Error: Faltan credenciales de DESTINO en el archivo .env (MONGO_USER2, MONGO_PASS2, MONGO_CLUSTER2)")
    sys.exit(1)

MONGO_URI_SOURCE = f"mongodb+srv://{db_user}:{db_pass}@{db_cluster}/?retryWrites=true&w=majority"
MONGO_URI_TARGET = f"mongodb+srv://{db_user2}:{db_pass2}@{db_cluster2}/?retryWrites=true&w=majority"

# Conexión a MongoDB ORIGEN
try:
    client_source = MongoClient(MONGO_URI_SOURCE)
    db_source = client_source[db_name_source]
    client_source.admin.command('ping')
    print("✅ Conexión exitosa a MongoDB (ORIGEN - Precios).")
except Exception as e:
    print(f"❌ Error crítico de conexión a MongoDB Origen: {e}")
    sys.exit(1)

# Conexión a MongoDB DESTINO
try:
    client_target = MongoClient(MONGO_URI_TARGET)
    db_target = client_target[db_name_target]
    client_target.admin.command('ping')
    print("✅ Conexión exitosa a MongoDB (DESTINO - Órdenes).")
except Exception as e:
    print(f"❌ Error crítico de conexión a MongoDB Destino: {e}")
    sys.exit(1)

# Definición de los exchanges conectando origen y destino según corresponda
EXCHANGES_CONFIG = [
    {
        "nombre": "bybit",
        "coleccion_precios": db_source["BBT_PRICE"],
        "coleccion_ordenes": db_target["1EXECUTED_ORDERS"]
    },
    {
        "nombre": "binance",
        "coleccion_precios": db_source["BNC_PRICE"],
        "coleccion_ordenes": db_target["1EXECUTED_ORDERS"]
    },
    {
        "nombre": "bingx",
        "coleccion_precios": db_source["BGX_PRICE"],
        "coleccion_ordenes": db_target["1EXECUTED_ORDERS"]
    },
    {
        "nombre": "mexc",
        "coleccion_precios": db_source["MXC_PRICE"],
        "coleccion_ordenes": db_target["1EXECUTED_ORDERS"]
    }
]

def extraer_comerciantes(lista_merchants):
    """Mapea los comerciantes usando su nickName y maneja tipos de datos mixtos."""
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
    eventos = []
    
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
    nombre_exchange = cfg["nombre"]
    col_price = cfg["coleccion_precios"]
    col_orders = cfg["coleccion_ordenes"]
    
    try:
        registros = list(col_price.find().sort("timestamp", -1).limit(2))
        
        if len(registros) < 2:
            return

        registro_actual = registros[0]
        registro_anterior = registros[1]

        id_actual = registro_actual["_id"]
        id_anterior = registro_anterior["_id"]

        existe_registro = col_orders.find_one({
            "exchange": nombre_exchange,
            "id_registro_actual": id_actual,
            "id_registro_anterior": id_anterior
        })

        if existe_registro:
            return

        buy_anterior = extraer_comerciantes(registro_anterior.get("merchant_buy", []))
        buy_actual = extraer_comerciantes(registro_actual.get("merchant_buy", []))
        
        sell_anterior = extraer_comerciantes(registro_anterior.get("merchant_sell", []))
        sell_actual = extraer_comerciantes(registro_actual.get("merchant_sell", []))

        if buy_anterior == buy_actual and sell_anterior == sell_actual:
            # print(f"[{nombre_exchange.upper()}] Sin cambios")
            return

        eventos_buy = comparar_seccion(buy_anterior, buy_actual, "merchant_buy", nombre_exchange)
        eventos_sell = comparar_seccion(sell_anterior, sell_actual, "merchant_sell", nombre_exchange)

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
            print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] 🔍 [{nombre_exchange.upper()}] Reducciones detectadas y guardadas en BD de destino.")

    except Exception as e:
        print(f"❌ Error procesando el exchange {nombre_exchange}: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando monitor P2P Multi-Cluster (Origen: Precios -> Destino: Órdenes)...")
    try:
        while True:
            for conf in EXCHANGES_CONFIG:
                procesar_exchange(conf)
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n🛑 Monitor detenido por el usuario.")
        client_source.close()
        client_target.close()
        sys.exit(0)