"""
ftp_watcher.py
==============
Monitor de cambios en el FTP. Cada 30 minutos:
  1. Escanea el SFTP buscando archivos nuevos SAV / AV / REFI / PL.
  2. Agrupa los archivos por horario (ej: PM_1400, AM_1100).
  3. Por cada grupo nuevo detectado → procesa SAV y AV juntos en una sola corrida.
  4. Notifica inicio y resultado en Microsoft Teams.

Nombre de archivo esperado:
    LEAKAGE_SAV_PM_1400_PARA_POBLAR_08062026_ENVIAR_2CALL.xlsx
    LEAKAGE_AV_PM_1400_PARA_POBLAR_08062026_ENVIAR_2CALL.xlsx
    └─ horario extraído: "PM_1400"  (o "AM_1100", etc.)

Reglas:
  - Si llega solo SAV o solo AV de un horario → se procesa igual, no espera al otro.
  - Si llegan los dos del mismo horario → se procesan juntos en una sola corrida.
  - Nunca se procesa el mismo horario dos veces.

Activar en main.py:
    from app.services.ftp_watcher import arrancar_watcher

    @app.on_event("startup")
    def startup():
        arrancar_watcher()

Variable .env requerida:
    TEAMS_WEBHOOK_URL = https://outlook.office.com/webhook/...
    INTERVALO_SEGUNDOS cambiar tiempo de sincronizacion
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime

import requests

from app.core.config import get_settings
from app.core.ftp import (
    _buscar_archivos_recursivo,
    _get_raiz_sftp,
    _get_sftp_config,
    get_sftp_client,
    descargar_archivo_sftp_por_nombre
)
from app.core.postgres import get_config_global, registrar_log, registrar_auditoria, get_config_usuario

logger   = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logging.getLogger("paramiko").setLevel(logging.WARNING)
logging.getLogger("paramiko.transport").setLevel(logging.WARNING)
settings = get_settings()

INTERVALO_SEGUNDOS = 60
CASOS              = ("SAV", "AV", "REFI", "PL")
SNAPSHOT_KEY       = "ftp_watcher_snapshot"

# Timestamp del momento exacto en que arrancó el watcher.
# Solo se procesan archivos con mtime >= este valor.
_arranque_ts: float = datetime.now().timestamp()

# ──────────────────────────────────────────────────────────────────────────────
# Estado en memoria — consultable vía GET /watcher/status
# ──────────────────────────────────────────────────────────────────────────────

_status: dict = {
    "activo":            False,
    "ultima_corrida":    None,   # ISO string
    "proxima_corrida":   None,   # ISO string
    "corridas_total":    0,
    "ultimo_resultado":  "—",    # "sin_archivos" | "procesado" | "error"
    "ultimo_error":      None,
    "archivos_en_ftp":   [],     # lista de dicts {tipo, nombre, horario, mtime_str}
    "historial":         [],     # últimas 20 corridas
}

def _status_push_historial(entrada: dict) -> None:
    _status["historial"].insert(0, entrada)
    _status["historial"] = _status["historial"][:20]

# Extrae el horario del nombre del archivo.
# Ej: "LEAKAGE_SAV_PM_1400_PARA_POBLAR_..." → "PM_1400"
_RE_HORARIO = re.compile(r'(?<![A-Z0-9])(AM|PM)_(\d{3,4})(?![A-Z0-9])', re.IGNORECASE)


# ──────────────────────────────────────────────────────────────────────────────
# Modelo
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ArchivoFTP:
    tipo:    str    # "SAV" | "AV" | "REFI" | "PL"
    nombre:  str    # nombre completo del archivo
    horario: str    # "PM_1400" | "AM_1100" | "SIN_HORARIO"
    mtime:   float

    def fecha_str(self) -> str:
        if self.mtime:
            return datetime.fromtimestamp(self.mtime).strftime("%d/%m/%Y %H:%M")
        return "—"


def _extraer_horario(nombre: str) -> str:
    """Extrae 'PM_1400' o 'AM_1100' del nombre del archivo. 'SIN_HORARIO' si no encuentra."""
    m = _RE_HORARIO.search(nombre.upper())
    if m:
        return f"{m.group(1).upper()}_{m.group(2).zfill(4)}"
    return "SIN_HORARIO"


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot — recuerda qué horarios ya fueron procesados
# ──────────────────────────────────────────────────────────────────────────────

def _cargar_snapshot() -> dict:
    """
    Estructura guardada:
    {
      "archivos":   { "LEAKAGE_SAV_PM_1400...xlsx": mtime, ... },
      "procesados": ["SAV_PM_1400_08062026", "AV_PM_1400_08062026", ...]
    }
    """
    cfg = get_config_global()
    raw = cfg.get(SNAPSHOT_KEY, "").strip()
    if not raw:
        return {"archivos": {}, "procesados": []}
    try:
        data = json.loads(raw)
        data.setdefault("archivos",   {})
        data.setdefault("procesados", [])
        return data
    except json.JSONDecodeError:
        logger.warning("[Watcher] Snapshot corrupto, se reinicia.")
        return {"archivos": {}, "procesados": []}


def _guardar_snapshot(snapshot: dict) -> None:
    try:
        from app.core.postgres import set_config_global
        set_config_global({SNAPSHOT_KEY: json.dumps(snapshot)})
    except ImportError:
        import pathlib
        pathlib.Path("/tmp/ftp_watcher_snapshot.json").write_text(
            json.dumps(snapshot, indent=2)
        )
def _marcar_procesado(clave: str) -> None:
    """Agrega una clave a procesados sin tocar archivos."""
    snapshot = _cargar_snapshot()
    if clave not in snapshot["procesados"]:
        snapshot["procesados"].append(clave)
        _guardar_snapshot(snapshot)


_RE_FECHA = re.compile(r'(\d{8})')

def _clave_procesado(tipo: str, horario: str, nombre_archivo: str = "") -> str:
    """Usa la fecha del nombre del archivo. Si no encuentra, usa la fecha de hoy."""
    m = _RE_FECHA.search(nombre_archivo)
    fecha = m.group(1) if m else datetime.now().strftime("%d%m%Y")
    return f"{tipo}_{horario}_{fecha}"


# ──────────────────────────────────────────────────────────────────────────────
# Escaneo del SFTP
# ──────────────────────────────────────────────────────────────────────────────

def _escanear_sftp() -> list[ArchivoFTP]:
    """Retorna todos los archivos xlsx/xls encontrados en el SFTP para todos los casos."""
    cfg      = _get_sftp_config()
    archivos = []
    try:
        ssh, sftp = get_sftp_client()
    except Exception as exc:
        logger.error("[Watcher] No se pudo conectar al SFTP: %s", exc)
        return archivos
    try:
        for tipo in CASOS:
            raiz    = _get_raiz_sftp(tipo)
            kw_glob = cfg["keyword_global"]
            kw_caso = cfg.get(f"keyword_{tipo}", tipo)
            encontrados = _buscar_archivos_recursivo(
                sftp, raiz, kw_glob, kw_caso, cfg["max_depth"]
            )
            for a in encontrados:
                archivos.append(ArchivoFTP(
                    tipo    = tipo,
                    nombre  = a.filename,
                    horario = _extraer_horario(a.filename),
                    mtime   = float(a.st_mtime or 0),
                ))
    finally:
        sftp.close(); ssh.close()
    return archivos


def _detectar_nuevos(
    archivos_actuales: list[ArchivoFTP],
    snapshot: dict,
) -> list[ArchivoFTP]:
    from datetime import date
    procesados = set(snapshot["procesados"])
    nuevos     = []

    hoy = date.today()

    for a in archivos_actuales:
        # Solo archivos de hoy
        if datetime.fromtimestamp(a.mtime).date() != hoy:
            continue

        clave = _clave_procesado(a.tipo, a.horario, a.nombre)
        if clave in procesados:
            continue  # ya fue procesado

        nuevos.append(a)

    return nuevos


# ──────────────────────────────────────────────────────────────────────────────
# Teams
# ──────────────────────────────────────────────────────────────────────────────

def _teams(titulo: str, mensaje: str, color: str = "0076D7") -> None:
    """
    Colores: "0076D7" azul, "FFA500" naranja, "28A745" verde, "DC3545" rojo
    """
    url = getattr(settings, "teams_webhook_url", "").strip()
    if not url:
        logger.warning("[Watcher] TEAMS_WEBHOOK_URL no configurado.")
        return
    payload = {
        "@type":      "MessageCard",
        "@context":   "https://schema.org/extensions",
        "themeColor": color,
        "summary":    titulo,
        "sections": [{
            "activityTitle":    f"**{titulo}**",
            "activitySubtitle": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "text":             mensaje,
        }],
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as exc:
        logger.error("[Watcher] Error enviando a Teams: %s", exc)


# ──────────────────────────────────────────────────────────────────────────────
# Procesamiento
# ──────────────────────────────────────────────────────────────────────────────

def _procesar_grupo(horario: str, archivos_grupo: list[ArchivoFTP]) -> None:
    """
    Procesa todos los archivos de un mismo horario (ej: PM_1400).
    SAV y AV se procesan en la misma corrida si llegaron juntos.
    """
    from app.core.postgres import get_config_global

    nombres = [a.nombre for a in archivos_grupo]

    logger.info("[Watcher] Procesando grupo %s: %s", horario, nombres)

    _teams(
        titulo  = f"⚙️ Procesando grupo {horario}",
        mensaje = (
            f"**Archivos detectados:** {', '.join(nombres)}  \n"
            f"**Horario:** {horario}  \n"
            "Iniciando procesamiento automático..."
        ),
        color="FFA500",
    )

    from app.core.postgres import get_config_usuario
    from datetime import date
    import os

    MESES = {
        1:"01-Enero",  2:"02-Febrero", 3:"03-Marzo",      4:"04-Abril",
        5:"05-Mayo",   6:"06-Junio",   7:"07-Julio",       8:"08-Agosto",
        9:"09-Septiembre", 10:"10-Octubre", 11:"11-Noviembre", 12:"12-Diciembre",
    }

    def _build_path_watcher(ruta_base: str, con_dia: bool = True) -> str:
        hoy = date.today()
        partes = [ruta_base, str(hoy.year), MESES[hoy.month]]
        if con_dia:
            partes.append(f"{hoy.day:02d}")
        ruta = os.path.join(*partes)
        os.makedirs(ruta, exist_ok=True)
        return ruta

    cfg             = get_config_global()
    USUARIO_WATCHER = "jorge.gomez"
    resultados      = []

    for archivo in archivos_grupo:
        tipo  = archivo.tipo
        u_cfg = get_config_usuario(USUARIO_WATCHER).get(tipo, {})

        destinos: dict[str, str] = {}
        if u_cfg.get("guardar_compartida", True):
            ruta_c = cfg.get(f"ruta_{tipo.lower()}_compartida", "")
            if ruta_c:
                destinos["compartida"] = _build_path_watcher(ruta_c)
        if u_cfg.get("guardar_local") and u_cfg.get("ruta_local"):
            destinos["local"] = u_cfg["ruta_local"]
        if not destinos:
            destinos["tmp"] = "/tmp"

        try:
            res = None

            # Descargar el archivo UNA sola vez desde el SFTP
            logger.info("[Watcher] Descargando %s desde SFTP: %s", tipo, archivo.nombre)
            from app.core.ftp import descargar_archivo_sftp
            archivo_bytes, nombre_archivo = descargar_archivo_sftp_por_nombre(tipo, archivo.nombre)

            for variante, output_dir in destinos.items():
                logger.info("[Watcher] %s → [%s] %s", tipo, variante, output_dir)

                import os
                ruta_base = os.path.join(output_dir, nombre_archivo)
                with open(ruta_base, "wb") as f:
                    f.write(archivo_bytes)
                logger.info("[Watcher] Archivo base guardado: %s", ruta_base)

                if tipo in ("SAV", "AV"):
                    from app.services.sav_av import procesar_sav_av
                    res = procesar_sav_av(
                        archivo_bytes=archivo_bytes,
                        nombre_archivo=nombre_archivo,
                        tipo=tipo,
                        output_dir=output_dir,
                    )
                elif tipo in ("REFI", "PL"):
                    from app.services.refi_pl import procesar_refi_pl
                    res = procesar_refi_pl(
                        archivo_bytes=archivo_bytes,
                        nombre_archivo=nombre_archivo,
                        tipo=tipo,
                        output_dir=output_dir,
                    )
                            # ── NUEVO: log de rutas generadas ──
                if res:
                    logger.info(
                        "[Watcher] Archivos generados en [%s] %s:\n"
                        "  Base    : %s\n"
                        "  Carga   : %s\n"
                        "  Bloqueo : %s\n"
                        "  Repetidos: %s",
                        variante,
                        output_dir,
                        ruta_base,
                        res.get("archivo_carga", "—"),
                        res.get("archivo_bloqueo", "—"),
                        res.get("archivo_repetidos", "—"),
                    )


            if res:
                resultados.append({
                    "tipo":    tipo,
                    "nombre":  res.get("_nombre_archivo", archivo.nombre),
                    "entrada": res.get("total_entrada", "—"),
                    "carga":   res.get("total_carga", "—"),
                    "rep":     res.get("total_repetidos", "—"),
                    "bloq":    res.get("total_bloqueados", "—"),
                    "error":   None,
                })

            registrar_auditoria(
                usuario="sistema",
                accion=f"autoproceso_{tipo.lower()}",
                detalle=f"Procesado automáticamente: {archivo.nombre} (horario {horario})",
            )

        except Exception as exc:
            logger.exception("[Watcher] Error procesando %s %s: %s", tipo, horario, exc)
            resultados.append({
                "tipo":   tipo,
                "nombre": archivo.nombre,
                "error":  str(exc),
            })

    # ── Armar mensaje de resultado para Teams ──
    lineas     = []
    hubo_error = False

    for r in resultados:
        if r.get("error"):
            hubo_error = True
            lineas.append(
                f"❌ **{r['tipo']}** — {r['nombre']}  \n"
                f"&nbsp;&nbsp;Error: {r['error']}"
            )
        else:
            lineas.append(
                f"✅ **{r['tipo']}** — {r['nombre']}  \n"
                f"&nbsp;&nbsp;Entrada: {r['entrada']} &nbsp;|&nbsp; "
                f"Carga: {r['carga']} &nbsp;|&nbsp; "
                f"Repetidos: {r['rep']} &nbsp;|&nbsp; "
                f"Bloqueados: {r['bloq']}"
            )

    _teams(
        titulo  = f"{'❌ Errores en' if hubo_error else '✅ Completado'} grupo {horario}",
        mensaje = "  \n".join(lineas),
        color   = "DC3545" if hubo_error else "28A745",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Ciclo principal
# ──────────────────────────────────────────────────────────────────────────────

def verificar_y_procesar() -> None:
    """
    Función principal. Se ejecuta cada INTERVALO_SEGUNDOS:
      1. Escanea el SFTP.
      2. Detecta archivos nuevos no procesados.
      3. Agrupa por horario.
      4. Procesa cada grupo y notifica en Teams.
    """
    ahora = datetime.now()
    logger.info("[Watcher] Revisando FTP — %s", ahora.strftime("%d/%m/%Y %H:%M"))

    _status["ultima_corrida"]  = ahora.isoformat()
    _status["corridas_total"] += 1
    _status["ultimo_error"]    = None

    snapshot = _cargar_snapshot()
    logger.info(
        "[Watcher] Snapshot cargado — archivos: %d, procesados: %d → %s",
        len(snapshot["archivos"]),
        len(snapshot["procesados"]),
        snapshot["procesados"],
    )

    archivos_actuales = _escanear_sftp()
    logger.info("[Watcher] SFTP escaneado — %d archivo(s) encontrado(s)", len(archivos_actuales))

    # Actualizar lista de archivos conocidos en el snapshot
    if archivos_actuales:
        for a in archivos_actuales:
            snapshot["archivos"][a.nombre] = a.mtime
        logger.info("[Watcher] Snapshot archivos actualizado — %d archivo(s)", len(snapshot["archivos"]))
    else:
        logger.warning("[Watcher] SFTP devolvió lista vacía — snapshot archivos NO se actualiza para evitar pisar datos")

    # Refrescar la lista visible en el status — archivos de hoy y ayer
    from datetime import date
    hoy_inicio_ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    _status["archivos_en_ftp"] = [
        {
            "tipo":    a.tipo,
            "nombre":  a.nombre,
            "horario": a.horario,
            "fecha":   a.fecha_str(),
            "mtime":   a.mtime,
        }
        for a in sorted(archivos_actuales, key=lambda a: a.mtime, reverse=True)
        if a.mtime >= hoy_inicio_ts  # ← solo hoy
    ]

    nuevos = _detectar_nuevos(archivos_actuales, snapshot)

    if not nuevos:
        logger.info("[Watcher] Sin archivos nuevos.")
        _status["ultimo_resultado"] = "sin_archivos"
        _status_push_historial({
            "ts":        ahora.isoformat(),
            "resultado": "sin_archivos",
            "detalle":   f"{len(archivos_actuales)} archivo(s) en FTP, ninguno nuevo.",
        })
        _guardar_snapshot(snapshot)
        logger.info("[Watcher] Snapshot guardado (sin archivos nuevos) — archivos: %d, procesados: %d", len(snapshot["archivos"]), len(snapshot["procesados"]))
        return

    # Agrupar por horario: {"PM_1400": [ArchivoFTP, ...], ...}
    grupos: dict[str, list[ArchivoFTP]] = {}
    for a in nuevos:
        grupos.setdefault(a.horario, []).append(a)

    logger.info(
        "[Watcher] %d archivo(s) nuevo(s) en %d grupo(s): %s",
        len(nuevos), len(grupos), list(grupos.keys()),
    )

    # Notificar detección en Teams
    lineas_deteccion = "\n".join(
        f"- **[{a.tipo}]** {a.nombre} ({a.fecha_str()})" for a in nuevos
    )
    _teams(
        titulo  = f"📂 {len(nuevos)} archivo(s) nuevo(s) detectado(s)",
        mensaje = lineas_deteccion,
        color   = "0076D7",
    )

    _status["ultimo_resultado"] = "procesado"
    _status_push_historial({
        "ts":        ahora.isoformat(),
        "resultado": "procesado",
        "detalle":   f"{len(nuevos)} nuevo(s): {[a.nombre for a in nuevos]}",
    })

    # Procesar cada grupo
    for horario, archivos_grupo in grupos.items():
        _procesar_grupo(horario, archivos_grupo)
        # Marcar como procesados
        for a in archivos_grupo:
            clave = _clave_procesado(a.tipo, a.horario, a.nombre)
            snapshot["procesados"].append(clave)
            logger.info("[Watcher] Marcado como procesado: %s", clave)

    logger.info(
        "[Watcher] Guardando snapshot — archivos: %d, procesados: %d → %s",
        len(snapshot["archivos"]),
        len(snapshot["procesados"]),
        snapshot["procesados"],
    )
    _guardar_snapshot(snapshot)
    logger.info("[Watcher] Snapshot guardado OK.")


# ──────────────────────────────────────────────────────────────────────────────
# Integración FastAPI + APScheduler
# ──────────────────────────────────────────────────────────────────────────────

import asyncio

async def _loop_watcher():
    logger.info("[Watcher] Loop iniciado, primera corrida en %ds", INTERVALO_SEGUNDOS)
    _status["activo"] = True
    while True:
        proxima = datetime.now().replace(microsecond=0)
        _status["proxima_corrida"] = proxima.isoformat()
        try:
            logger.info("[Watcher] Disparando verificacion...")
            # ← CAMBIO: correr en thread para no bloquear el event loop
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, verificar_y_procesar)
        except Exception as exc:
            logger.exception("[Watcher] Error: %s", exc)
            _status["ultimo_resultado"] = "error"
            _status["ultimo_error"]     = str(exc)
            _status_push_historial({
                "ts":        datetime.now().isoformat(),
                "resultado": "error",
                "detalle":   str(exc),
            })
        proxima_real = datetime.fromtimestamp(
            datetime.now().timestamp() + INTERVALO_SEGUNDOS
        )
        _status["proxima_corrida"] = proxima_real.isoformat()
        logger.info("[Watcher] Esperando %ds para proxima corrida...", INTERVALO_SEGUNDOS)
        await asyncio.sleep(INTERVALO_SEGUNDOS)

def arrancar_watcher():
    logger.info("[Watcher] Registrando tarea en event loop...")
    asyncio.ensure_future(_loop_watcher())
    logger.info("[Watcher] Tarea registrada OK.")


def get_watcher_status() -> dict:
    """
    Devuelve el estado actual del watcher.
    Llamado desde GET /watcher/status en main.py.
    Incluye 'procesados' del snapshot para cruzar en el frontend.
    """
    import copy
    data = copy.deepcopy(_status)
    try:
        snapshot = _cargar_snapshot()
        data["procesados"] = snapshot.get("procesados", [])
    except Exception:
        data["procesados"] = []
    return data

# ──────────────────────────────────────────────────────────────────────────────
# Proceso independiente
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
    )
    while True:
        try:
            verificar_y_procesar()
        except Exception as exc:
            logger.exception("[Watcher] Error inesperado: %s", exc)
        time.sleep(INTERVALO_SEGUNDOS)