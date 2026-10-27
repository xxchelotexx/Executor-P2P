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

# Definir la zona horaria UTC-4
UTC_MINUS_4 = timezone(timedelta(hours=-4))

# Variable global para mantener referencia al proceso del collector
collector_process = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global collector_process
    print("🚀 Iniciando collector.py en segundo plano...")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    collector_path = os.path.join(current_dir, "collector.py")
    
    collector_process = subprocess.Popen([sys.executable, collector_path])
    
    yield
    
    if collector_process:
        print("🛑 Deteniendo collector.py...")
        collector_process.terminate()
        collector_process.wait()

app = FastAPI(title="API Monitor P2P Bolivia", version="1.6.0", lifespan=lifespan)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

client = AsyncIOMotorClient(MONGO_URI)
db = client["P2P_Data"]
collection = db["1EXECUTED_ORDERS"]

def procesar_y_agrupar_eventos(resultados):
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
        hora_fin_local = datetime.now(UTC_MINUS_4)
        hora_inicio_local = hora_fin_local - timedelta(minutes=minutos)

        hora_fin_utc = hora_fin_local.astimezone(timezone.utc)
        hora_inicio_utc = hora_inicio_local.astimezone(timezone.utc)

        cursor = collection.find(
            {"timestamp_analisis": {"$gte": hora_inicio_utc, "$lte": hora_fin_utc}}
        ).sort("timestamp_analisis", -1)
        
        resultados = await cursor.to_list(length=500)
        
        compras, ventas, liquidados_apagados = procesar_y_agrupar_eventos(resultados)

        volumen_compras = sum(e.get("cantidad_transaccionada", 0) for e in compras)
        volumen_ventas = sum(e.get("cantidad_transaccionada", 0) for e in ventas)
        volumen_liquidados = sum(e.get("cantidad_transaccionada", 0) for e in liquidados_apagados)

        # total_registros ahora suma compras y ventas
        total_compras_len = len(compras)
        total_ventas_len = len(ventas)

        return {
            "minutos_solicitados": minutos,
            "Hora_inicio_UTC_minus_4": hora_inicio_local.strftime("%Y-%m-%d %H:%M:%S"),
            "Hora_fin_UTC_minus_4": hora_fin_local.strftime("%Y-%m-%d %H:%M:%S"),
            "total_registros": total_compras_len + total_ventas_len,
            "total_compras": total_compras_len,
            "total_ventas": total_ventas_len,
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
    fecha_inicio: str = Query(..., description="Fecha y hora de inicio en formato YYYY-MM-DD HH:MM:SS (Ej: 2026-10-27 05:15:00)"),
    fecha_fin: str = Query(..., description="Fecha y hora de fin en formato YYYY-MM-DD HH:MM:SS (Ej: 2026-10-27 23:59:59)")
):
    try:
        try:
            dt_inicio_local = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC_MINUS_4)
            dt_fin_local = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC_MINUS_4)

            dt_inicio_utc = dt_inicio_local.astimezone(timezone.utc)
            dt_fin_utc = dt_fin_local.astimezone(timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Formato de fecha inválido. Utilice estrictamente el formato: YYYY-MM-DD HH:MM:SS (ej. 2026-10-27 05:15:00)"
            )

        cursor = collection.find({
            "timestamp_analisis": {
                "$gte": dt_inicio_utc,
                "$lte": dt_fin_utc
            }
        }).sort("timestamp_analisis", -1)

        resultados = await cursor.to_list(length=500)
        compras, ventas, liquidados_apagados = procesar_y_agrupar_eventos(resultados)

        volumen_compras = sum(e.get("cantidad_transaccionada", 0) for e in compras)
        volumen_ventas = sum(e.get("cantidad_transaccionada", 0) for e in ventas)
        volumen_liquidados = sum(e.get("cantidad_transaccionada", 0) for e in liquidados_apagados)

        total_compras_len = len(compras)
        total_ventas_len = len(ventas)

        return {
            "Hora_inicio_UTC_minus_4": dt_inicio_local.strftime("%Y-%m-%d %H:%M:%S"),
            "Hora_fin_UTC_minus_4": dt_fin_local.strftime("%Y-%m-%d %H:%M:%S"),
            "total_registros": total_compras_len + total_ventas_len,
            "total_compras": total_compras_len,
            "total_ventas": total_ventas_len,
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