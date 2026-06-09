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
async def procesar_sav(
    archivo: UploadFile = File(...),
    txt_resoluciones: UploadFile = File(None),
):
    """
    Procesa el archivo SAV Leakage.
    Opcionalmente acepta el TXT de resoluciones; si no se sube, lo descarga del FTP.
    """
    try:
        _verificar_no_procesado("SAV", archivo.filename)
        contenido = await archivo.read()
        txt_bytes = await txt_resoluciones.read() if txt_resoluciones else None
        txt_nombre = txt_resoluciones.filename if txt_resoluciones else None
        resultado = procesar_sav_av(
            contenido, archivo.filename, "SAV", TEMP_DIR,
            txt_resoluciones_bytes=txt_bytes,
            txt_resoluciones_nombre=txt_nombre,
        )
        return _respuesta_leakage(resultado)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# AV
# ─────────────────────────────────────────────

@router.post("/av/procesar")
async def procesar_av(
    archivo: UploadFile = File(...),
    txt_resoluciones: UploadFile = File(None),
):
    """
    Procesa el archivo AV Leakage.
    Opcionalmente acepta el TXT de resoluciones; si no se sube, lo descarga del FTP.
    """
    try:
        _verificar_no_procesado("AV", archivo.filename)
        contenido = await archivo.read()
        txt_bytes = await txt_resoluciones.read() if txt_resoluciones else None
        txt_nombre = txt_resoluciones.filename if txt_resoluciones else None
        resultado = procesar_sav_av(
            contenido, archivo.filename, "AV", TEMP_DIR,
            txt_resoluciones_bytes=txt_bytes,
            txt_resoluciones_nombre=txt_nombre,
        )
        return _respuesta_leakage(resultado)
    except HTTPException:
        raise
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
        _verificar_no_procesado("REFI", archivo.filename if archivo else None)
        if archivo:
            contenido = await archivo.read()
            resultado = procesar_refi_pl("REFI", TEMP_DIR, contenido, archivo.filename)
        else:
            resultado = procesar_refi_pl("REFI", TEMP_DIR)
        return _respuesta_leakage(resultado)
    except HTTPException:
        raise
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
        _verificar_no_procesado("PL", archivo.filename if archivo else None)
        if archivo:
            contenido = await archivo.read()
            resultado = procesar_refi_pl("PL", TEMP_DIR, contenido, archivo.filename)
        else:
            resultado = procesar_refi_pl("PL", TEMP_DIR)
        return _respuesta_leakage(resultado)
    except HTTPException:
        raise
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

def _verificar_no_procesado(tipo: str, nombre_archivo) -> None:
    """
    Lanza HTTP 409 si el archivo ya fue procesado hoy según el snapshot del watcher.
    El frontend muestra el detail como "Base X ya Procesada".
    """
    try:
        from app.core.ftp_watcher import (
            _cargar_snapshot, _clave_procesado, _extraer_horario
        )
        horario  = _extraer_horario(nombre_archivo or "")
        snapshot = _cargar_snapshot()
        clave    = _clave_procesado(tipo, horario)
        if clave in snapshot.get("procesados", []):
            # Construir nombre legible: "SAV PM_1700"
            nombre_base = f"{tipo} {horario}" if horario != "SIN_HORARIO" else tipo
            raise HTTPException(
                status_code=409,
                detail=f"Base {nombre_base} ya Procesada"
            )
    except HTTPException:
        raise
    except Exception:
        pass  # Si falla la consulta al snapshot, dejar pasar el proceso


def _respuesta_leakage(resultado: dict) -> dict:
    respuesta = {
        "mensaje": "Archivo procesado correctamente",
        "resumen": {
            "total_entrada":           resultado["total_entrada"],
            "total_repetidos":         resultado["total_repetidos"],
            "total_bloqueados":        resultado["total_bloqueados"],
            "total_carga":             resultado["total_carga"],
        },
        "archivos": {
            "carga":      f"/api/v1/descargar?path={resultado['archivo_carga']}",
            "repetidos":  f"/api/v1/descargar?path={resultado['archivo_repetidos']}",
            "bloqueo":    f"/api/v1/descargar?path={resultado['archivo_bloqueo']}",
        }
    }

    # Descartados por monto (SAV / AV)
    if resultado.get("total_descartados_monto") is not None:
        respuesta["resumen"]["total_descartados_monto"] = resultado["total_descartados_monto"]
        respuesta["archivos"]["descartados_monto"] = (
            f"/api/v1/descargar?path={resultado['archivo_descartados_monto']}"
        )

    # Resoluciones: solo se incluye si el proceso las generó
    if "total_resoluciones" in resultado:
        respuesta["resumen"]["total_resoluciones"] = resultado["total_resoluciones"]
        respuesta["archivos"]["resoluciones"] = (
            f"/api/v1/descargar?path={resultado['archivo_resoluciones']}"
        )

    return respuesta