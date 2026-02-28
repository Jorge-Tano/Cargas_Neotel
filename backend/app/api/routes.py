from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse
import tempfile
import os

from app.services.perdidas import procesar_llamadas_perdidas
from app.services.sav_av import procesar_sav_av
from app.services.refi_pl import procesar_refi_pl
from app.services.lista_negra import procesar_lista_negra

router = APIRouter(prefix="/api/v1")

# Directorio temporal para archivos generados
TEMP_DIR = tempfile.gettempdir()


# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────

@router.get("/health")
def health():
    return {"status": "ok", "message": "Sistema Neotel funcionando"}


# ─────────────────────────────────────────────
# LISTA NEGRA
# ─────────────────────────────────────────────

@router.post("/lista-negra/actualizar")
async def actualizar_lista_negra(archivo: UploadFile = File(...)):
    """
    Sube el archivo BlackList del día y sincroniza con PostgreSQL.
    """
    try:
        contenido = await archivo.read()
        resultado = procesar_lista_negra(contenido, archivo.filename)
        return {
            "mensaje": "Lista negra actualizada correctamente",
            **resultado
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# LLAMADAS PERDIDAS
# ─────────────────────────────────────────────

@router.post("/perdidas/procesar")
async def procesar_perdidas(archivo: UploadFile = File(...)):
    """
    Procesa el archivo de llamadas perdidas.
    Retorna los archivos generados para descargar.
    """
    try:
        contenido = await archivo.read()
        resultado = procesar_llamadas_perdidas(contenido, archivo.filename, TEMP_DIR)
        return {
            "mensaje": "Archivo procesado correctamente",
            "total_entrada": resultado["total_entrada"],
            "total_carga": resultado["total_carga"],
            "archivos": {
                "carga": f"/api/v1/descargar?path={resultado['archivo_carga']}"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# SAV
# ─────────────────────────────────────────────

@router.post("/sav/procesar")
async def procesar_sav(archivo: UploadFile = File(...)):
    """
    Procesa el archivo SAV Leakage.
    """
    try:
        contenido = await archivo.read()
        resultado = procesar_sav_av(contenido, archivo.filename, "SAV", TEMP_DIR)
        return _respuesta_leakage(resultado)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# AV
# ─────────────────────────────────────────────

@router.post("/av/procesar")
async def procesar_av(archivo: UploadFile = File(...)):
    """
    Procesa el archivo AV Leakage.
    """
    try:
        contenido = await archivo.read()
        resultado = procesar_sav_av(contenido, archivo.filename, "AV", TEMP_DIR)
        return _respuesta_leakage(resultado)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# REFI
# ─────────────────────────────────────────────

@router.post("/refi/procesar")
async def procesar_refi(archivo: UploadFile = File(None)):
    """
    Procesa REFI Leakage.
    Si no se sube archivo, lo descarga automáticamente desde FTP.
    """
    try:
        if archivo:
            contenido = await archivo.read()
            resultado = procesar_refi_pl("REFI", TEMP_DIR, contenido, archivo.filename)
        else:
            resultado = procesar_refi_pl("REFI", TEMP_DIR)
        return _respuesta_leakage(resultado)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# PL (PAGO LIVIANO)
# ─────────────────────────────────────────────

@router.post("/pl/procesar")
async def procesar_pl(archivo: UploadFile = File(None)):
    """
    Procesa PL Leakage.
    Si no se sube archivo, lo descarga automáticamente desde FTP.
    """
    try:
        if archivo:
            contenido = await archivo.read()
            resultado = procesar_refi_pl("PL", TEMP_DIR, contenido, archivo.filename)
        else:
            resultado = procesar_refi_pl("PL", TEMP_DIR)
        return _respuesta_leakage(resultado)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# DESCARGA DE ARCHIVOS GENERADOS
# ─────────────────────────────────────────────

@router.get("/descargar")
def descargar_archivo(path: str):
    """
    Descarga un archivo generado por el sistema.
    """
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    return FileResponse(
        path=path,
        filename=os.path.basename(path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _respuesta_leakage(resultado: dict) -> dict:
    return {
        "mensaje": "Archivo procesado correctamente",
        "resumen": {
            "total_entrada": resultado["total_entrada"],
            "total_repetidos": resultado["total_repetidos"],
            "total_bloqueados": resultado["total_bloqueados"],
            "total_carga": resultado["total_carga"],
        },
        "archivos": {
            "carga": f"/api/v1/descargar?path={resultado['archivo_carga']}",
            "repetidos": f"/api/v1/descargar?path={resultado['archivo_repetidos']}",
            "bloqueo": f"/api/v1/descargar?path={resultado['archivo_bloqueo']}",
        }
    }
