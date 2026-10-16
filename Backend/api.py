import os
import sys
import subprocess
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# Cargar variables de entorno
load_dotenv()

MONGO_USER = os.getenv("MONGO_USER2")
MONGO_PASS = os.getenv("MONGO_PASS2")
MONGO_CLUSTER = os.getenv("MONGO_CLUSTER2")

MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_CLUSTER}/?retryWrites=true&w=majority"

# Variable global para mantener referencia al proceso del collector
collector_process = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global collector_process
    # Se ejecuta al iniciar la API
    print("🚀 Iniciando collector.py en segundo plano...")
    
    # Resolver la ruta absoluta de collector.py de manera dinámica y segura para Windows/Linux
    current_dir = os.path.dirname(os.path.abspath(__file__))
    collector_path = os.path.join(current_dir, "collector.py")
    
    # Usamos sys.executable para asegurar que use el mismo entorno virtual de Python
    collector_process = subprocess.Popen([sys.executable, collector_path])
    
    yield
    
    # Se ejecuta al apagar la API
    if collector_process:
        print("🛑 Deteniendo collector.py...")
        collector_process.terminate()
        collector_process.wait()

# Pasamos el lifespan a la aplicación FastAPI
app = FastAPI(title="API Monitor P2P Bolivia", version="1.6.0", lifespan=lifespan)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir solicitudes desde cualquier origen (ideal para desarrollo)
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permitir todas las cabeceras
)

client = AsyncIOMotorClient(MONGO_URI)
db = client["P2P_Data"]
collection = db["1EXECUTED_ORDERS"]

def procesar_y_agrupar_eventos(resultados):
    """
    Recorre todos los documentos obtenidos de la base de datos y extrae,
    limpia y agrupa todos los eventos en tres listas globales consolidadas.
    """
    compras = []
    ventas = []
    liquidados_apagados = []

    for doc in resultados:
        buy_eventos = doc.get("merchant_buy_eventos") or []
        sell_eventos = doc.get("merchant_sell_eventos") or []
        eventos_totales = buy_eventos + sell_eventos

        for evento in eventos_totales:
            if not evento:
                continue
            
            # Asegurar que cantidad_transaccionada sea float para sumar volúmenes correctamente
            if "cantidad_transaccionada" in evento and evento["cantidad_transaccionada"] is not None:
                try:
                    evento["cantidad_transaccionada"] = float(evento["cantidad_transaccionada"])
                except ValueError:
                    pass

            accion = evento.get("accion", "").lower()

            if "liquidado" in accion or "apagado" in accion:
                liquidados_apagados.append(evento)
            elif "venta" in accion or "sell" in accion:
                ventas.append(evento)
            elif "compra" in accion or "buy" in accion:
                compras.append(evento)
            else:
                ventas.append(evento)

    return compras, ventas, liquidados_apagados

@app.get("/ultimas_ordenes")
async def get_ultimas_ordenes(
    minutos: int = Query(..., description="Minutos hacia atrás a consultar (ej: 5, 10, 15, 30, 60)", enum=[5, 10, 15, 30, 60])
):
    try:
        hora_fin = datetime.now(timezone.utc)
        hora_inicio = hora_fin - timedelta(minutes=minutos)

        cursor = collection.find(
            {"timestamp_analisis": {"$gte": hora_inicio, "$lte": hora_fin}}
        ).sort("timestamp_analisis", -1)
        
        resultados = await cursor.to_list(length=500)
        
        compras, ventas, liquidados_apagados = procesar_y_agrupar_eventos(resultados)

        volumen_compras = sum(e.get("cantidad_transaccionada", 0) for e in compras)
        volumen_ventas = sum(e.get("cantidad_transaccionada", 0) for e in ventas)
        volumen_liquidados = sum(e.get("cantidad_transaccionada", 0) for e in liquidados_apagados)

        return {
            "minutos_solicitados": minutos,
            "Hora_inicio": hora_inicio.strftime("%Y-%m-%d %H:%M:%S"),
            "Hora_fin": hora_fin.strftime("%Y-%m-%d %H:%M:%S"),
            "total_registros": len(resultados),
            "total_compras": len(compras),
            "total_ventas": len(ventas),
            "total_liquidados_apagados": len(liquidados_apagados),
            "volumen_compras": round(volumen_compras, 2),
            "volumen_ventas": round(volumen_ventas, 2),
            "volumen_liquidados_apagados": round(volumen_liquidados, 2),
            "data": {
                "ventas": ventas,
                "compras": compras,
                "Liquidado/Apagado": liquidados_apagados
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ordenes")
async def get_ordenes_por_rango(
    fecha_inicio: str = Query(..., description="Fecha y hora de inicio en formato YYYY-MM-DD HH:MM:SS (Ej: 2026-10-15 17:00:00)"),
    fecha_fin: str = Query(..., description="Fecha y hora de fin en formato YYYY-MM-DD HH:MM:SS (Ej: 2026-10-15 17:05:00)")
):
    try:
        try:
            # Parsear las fechas usando el formato estricto solicitado
            dt_inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            dt_fin = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Formato de fecha inválido. Utilice estrictamente el formato: YYYY-MM-DD HH:MM:SS (ej. 2026-10-15 17:00:00)"
            )

        cursor = collection.find({
            "timestamp_analisis": {
                "$gte": dt_inicio,
                "$lte": dt_fin
            }
        }).sort("timestamp_analisis", -1)

        resultados = await cursor.to_list(length=500)
        compras, ventas, liquidados_apagados = procesar_y_agrupar_eventos(resultados)

        volumen_compras = sum(e.get("cantidad_transaccionada", 0) for e in compras)
        volumen_ventas = sum(e.get("cantidad_transaccionada", 0) for e in ventas)
        volumen_liquidados = sum(e.get("cantidad_transaccionada", 0) for e in liquidados_apagados)

        return {
            "Hora_inicio": dt_inicio.strftime("%Y-%m-%d %H:%M:%S"),
            "Hora_fin": dt_fin.strftime("%Y-%m-%d %H:%M:%S"),
            "total_registros": len(resultados),
            "total_compras": len(compras),
            "total_ventas": len(ventas),
            "total_liquidados_apagados": len(liquidados_apagados),
            "volumen_compras": round(volumen_compras, 2),
            "volumen_ventas": round(volumen_ventas, 2),
            "volumen_liquidados_apagados": round(volumen_liquidados, 2),
            "data": {
                "ventas": ventas,
                "compras": compras,
                "Liquidado/Apagado": liquidados_apagados
            }
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="127.0.0.1", port=5000, reload=True)