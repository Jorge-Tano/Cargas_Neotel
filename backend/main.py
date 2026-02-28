"""
Backend FastAPI - Neotel Cargas
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from datetime import date
import os

app = FastAPI(title="Neotel Cargas API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Ruta base configurable ────────────────────────────────
BASE_OUTPUT = r"D:\Cargas\Leakage"

MESES = {
    1:"01-Enero", 2:"02-Febrero", 3:"03-Marzo", 4:"04-Abril",
    5:"05-Mayo", 6:"06-Junio", 7:"07-Julio", 8:"08-Agosto",
    9:"09-Septiembre", 10:"10-Octubre", 11:"11-Noviembre", 12:"12-Diciembre"
}


def get_output_dir(tipo: str) -> str:
    hoy = date.today()
    path = os.path.join(BASE_OUTPUT, tipo, str(hoy.year), MESES[hoy.month], str(hoy.day))
    os.makedirs(path, exist_ok=True)
    return path


def _archivos_generados(resultado: dict) -> list:
    archivos = []
    for key, path in resultado.items():
        if key.startswith("archivo") and path and os.path.exists(path):
            archivos.append({"nombre": os.path.basename(path), "path": path})
    return archivos


# ── Endpoints ─────────────────────────────────────────────

@app.post("/procesar/sav")
async def procesar_sav(file: UploadFile = File(...)):
    from app.services.sav_av import procesar_sav_av
    contenido = await file.read()
    resultado = procesar_sav_av(contenido, file.filename, "SAV", get_output_dir("SAV"))
    resultado["archivos"] = _archivos_generados(resultado)
    return resultado


@app.post("/procesar/av")
async def procesar_av(file: UploadFile = File(...)):
    from app.services.sav_av import procesar_sav_av
    contenido = await file.read()
    resultado = procesar_sav_av(contenido, file.filename, "AV", get_output_dir("AV"))
    resultado["archivos"] = _archivos_generados(resultado)
    return resultado


@app.post("/procesar/refi")
async def procesar_refi():
    from app.services.refi_pl import procesar_refi_pl
    resultado = procesar_refi_pl(tipo="REFI", output_dir=get_output_dir("REFI"))
    resultado["archivos"] = _archivos_generados(resultado)
    return resultado


@app.post("/procesar/pl")
async def procesar_pl():
    from app.services.refi_pl import procesar_refi_pl
    resultado = procesar_refi_pl(tipo="PL", output_dir=get_output_dir("PL"))
    resultado["archivos"] = _archivos_generados(resultado)
    return resultado


@app.post("/procesar/perdidas")
async def procesar_perdidas(file: UploadFile = File(...)):
    from app.services.perdidas import procesar_llamadas_perdidas
    contenido = await file.read()
    resultado = procesar_llamadas_perdidas(contenido, file.filename, get_output_dir("PERDIDAS"))
    resultado["archivos"] = _archivos_generados(resultado)
    return resultado


@app.post("/lista-negra/actualizar")
async def actualizar_lista_negra():
    from app.services.lista_negra import procesar_lista_negra
    return procesar_lista_negra()


@app.get("/logs")
async def get_logs(limit: int = 50):
    from app.core.postgres import get_logs
    return get_logs(limit=limit)


@app.get("/config/ruta-base")
async def get_ruta_base():
    return {"ruta_base": BASE_OUTPUT}


@app.put("/config/ruta-base")
async def set_ruta_base(body: dict):
    global BASE_OUTPUT
    nueva_ruta = body.get("ruta_base", "").strip()
    if not nueva_ruta:
        raise HTTPException(status_code=400, detail="Ruta vacía")
    BASE_OUTPUT = nueva_ruta
    os.makedirs(nueva_ruta, exist_ok=True)
    return {"ruta_base": BASE_OUTPUT, "ok": True}


@app.get("/descargar")
async def descargar_archivo(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(path, filename=os.path.basename(path))


@app.get("/health")
async def health():
    return {"status": "ok", "ruta_base": BASE_OUTPUT}