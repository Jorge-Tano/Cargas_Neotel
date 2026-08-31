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
    _kw_global_para,
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
CASOS              = ("SAV", "AV", "REFI", "PL", "CARRITO", "MKT")
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
    tipo:    str    # "SAV" | "AV" | "REFI" | "PL" | "MKT"
    nombre:  str    # nombre completo del archivo
    horario: str    # "PM_1400" | "AM_1100" | "SIN_HORARIO"
    mtime:   float

    def fecha_str(self) -> str:
        if self.mtime:
            return datetime.fromtimestamp(self.mtime).strftime("%d/%m/%Y %H:%M")
        return "—"


_RE_TIMESTAMP_12 = re.compile(r'(\d{12})')  # ej: 202608211609 (AAAAMMDDHHMM)


def _extraer_horario(nombre: str) -> str:
    """
    Extrae 'PM_1400' o 'AM_1100' del nombre del archivo (casos Leakage).
    Si no encuentra ese patrón, intenta usar un timestamp de 12 dígitos
    (AAAAMMDDHHMM) como el de Carrito Abandonado, para que cada corrida
    horaria tenga una clave única y no se pisen entre sí en el snapshot.
    'SIN_HORARIO' si no encuentra ninguno de los dos.
    """
    m = _RE_HORARIO.search(nombre.upper())
    if m:
        return f"{m.group(1).upper()}_{m.group(2).zfill(4)}"
    m2 = _RE_TIMESTAMP_12.search(nombre)
    if m2:
        return m2.group(1)
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
            kw_glob = _kw_global_para(cfg, tipo)
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
        logger.info("[Watcher] Teams respondió %s: %s", r.status_code, r.text[:200])
        r.raise_for_status()
    except Exception as exc:
        logger.error("[Watcher] Error enviando a Teams: %s", exc)


# ──────────────────────────────────────────────────────────────────────────────
# Procesamiento
# ──────────────────────────────────────────────────────────────────────────────

# Etiqueta del campo "bloq" en la notificación de Teams. Por defecto
# significa lista negra ("Bloqueados"), pero en casos donde ese número
# representa otra cosa (ej. CARRITO = registros sin teléfono válido)
# se puede personalizar el texto sin tocar la lógica de armado del mensaje.
_ETIQUETA_BLOQUEADOS = {
    "CARRITO": "📵 Sin teléfono",
    "MKT":     "📵 Sin teléfono",

}


def _etiqueta_bloqueados(tipo: str) -> str:
    return _ETIQUETA_BLOQUEADOS.get(tipo, "🚫 Bloqueados")


# Etiqueta del campo "excluidos" (registros descartados antes de la carga
# por no cumplir el filtro de oferta/monto). Varía el texto según el caso
# para que sea claro qué se excluyó, sin tocar la lógica de armado.
_ETIQUETA_EXCLUIDOS = {
    "SAV": "💰 Desc. monto",
    "AV":  "💰 Desc. monto",
    "PL":  "💰 Desc. deuda",
}


def _etiqueta_excluidos(tipo: str) -> str:
    return _ETIQUETA_EXCLUIDOS.get(tipo, "🚫 Excluidos")


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
                f"Casos: {', '.join(f'<b>{a.tipo}</b>' for a in archivos_grupo)}"
                f"<br>⏱️ Iniciando procesamiento automático..."
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

    cfg = get_config_global()
    # El watcher corre sin sesión de usuario, así que necesita saber de qué
    # cuenta leer la config de rutas locales (guardar_local/ruta_local por
    # tipo). Antes estaba hardcodeado a "jorge.gomez": si alguien configuraba
    # las rutas (Configuración → Rutas) logueado con OTRA cuenta, el watcher
    # nunca veía esa config y esos casos no le llegaban a "local" -aunque en
    # la UI se vieran guardadas-. Ahora se puede fijar la cuenta correcta en
    # config_global (clave "usuario_watcher_rutas"), sin tocar código; si no
    # está configurada, se mantiene "jorge.gomez" como fallback.
    USUARIO_WATCHER = cfg.get("usuario_watcher_rutas", "").strip() or "jorge.gomez"
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

        # Asegurar que todos los directorios destino existan (evita
        # FileNotFoundError en Windows con rutas tipo "/tmp" o rutas
        # locales que aún no se hayan creado).
        for _ruta_destino in destinos.values():
            os.makedirs(_ruta_destino, exist_ok=True)

        # Carpeta donde se guarda el archivo base descargado del SFTP.
        # Prioridad compartida > local > tmp (si no hay ninguna configurada).
        import tempfile
        if destinos.get("compartida"):
            dir_base = destinos["compartida"]
        elif destinos.get("local"):
            dir_base = destinos["local"]
        else:
            dir_base = tempfile.gettempdir()
            os.makedirs(dir_base, exist_ok=True)

        try:
            res = None

            # Descargar el archivo UNA sola vez desde el SFTP
            logger.info("[Watcher] Descargando %s desde SFTP: %s", tipo, archivo.nombre)
            from app.core.ftp import descargar_archivo_sftp
            archivo_bytes, nombre_archivo = descargar_archivo_sftp_por_nombre(tipo, archivo.nombre)

            import os, shutil

            ruta_base = os.path.join(dir_base, nombre_archivo)
            with open(ruta_base, "wb") as f:
                f.write(archivo_bytes)
            logger.info("[Watcher] Archivo base guardado: %s", ruta_base)
            logger.info("[Watcher] %s → destinos: %s", tipo, destinos)

            # ── Procesar una sola vez: cada servicio decide internamente
            #    qué archivos van a "compartida" (todos) y cuáles también
            #    a "local" (solo Carga y Bloqueo) ──
            if tipo in ("SAV", "AV"):
                from app.services.sav_av import procesar_sav_av
                res = procesar_sav_av(
                    archivo_bytes=archivo_bytes,
                    nombre_archivo=nombre_archivo,
                    tipo=tipo,
                    output_dirs=destinos,
                )
            elif tipo in ("REFI", "PL"):
                from app.services.refi_pl import procesar_refi_pl
                res = procesar_refi_pl(
                    archivo_bytes=archivo_bytes,
                    nombre_archivo=nombre_archivo,
                    tipo=tipo,
                    output_dirs=destinos,
                )
            elif tipo == "CARRITO":
                from app.services.carrito_abandonado import procesar_carrito_abandonado
                res = procesar_carrito_abandonado(
                    archivo_bytes=archivo_bytes,
                    nombre_archivo=nombre_archivo,
                    output_dirs=destinos,
                )
            elif tipo == "MKT":
                from app.services.mkt import procesar_mkt
                res = procesar_mkt(
                    archivo_bytes=archivo_bytes,
                    nombre_archivo=nombre_archivo,
                    output_dirs=destinos,
                )

            if res:
                logger.info(
                    "[Watcher] Archivos generados para %s:\n"
                    "  Base    : %s\n"
                    "  Carga   : %s\n"
                    "  Bloqueo : %s\n"
                    "  Repetidos: %s",
                    tipo,
                    ruta_base,
                    res.get("archivo_carga", "—"),
                    res.get("archivo_bloqueo", "—"),
                    res.get("archivo_repetidos", "—"),
                )

            # ── Copiar el archivo base (no los generados, esos ya los
            #    maneja cada servicio) al resto de destinos configurados ──
            for variante, ruta_destino in destinos.items():
                if ruta_destino == dir_base:
                    continue
                logger.info("[Watcher] %s → [%s] %s (copia de archivo base)", tipo, variante, ruta_destino)
                try:
                    shutil.copy2(ruta_base, os.path.join(ruta_destino, nombre_archivo))
                except Exception as exc_copy:
                    logger.exception("[Watcher] Error copiando archivo base a [%s] %s: %s", variante, ruta_destino, exc_copy)

            if res:
                resultados.append({
                    "tipo":     tipo,
                    "nombre":   res.get("_nombre_archivo", archivo.nombre),
                    "entrada":  res.get("total_entrada", "—"),
                    "carga":    res.get("total_carga", "—"),
                    "rep":      res.get("total_repetidos", "—"),
                    "bloq":     res.get("total_bloqueados", "—"),
                    "agendas":  res.get("total_resoluciones", "—"),
                    "excl":     res.get("total_descartados_monto", res.get("total_excluidos", "—")),
                    "error":    None,
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
                f"❌ <b>{r['tipo']}</b><br>"
                f"&nbsp;&nbsp;&nbsp;&nbsp;Error: {r['error']}"
            )
        else:
            # Solo se muestran los campos con valor > 0 (o "—"/desconocido).
            # 📤 Carga siempre se muestra, aunque sea 0.
            def _oculto(v):
                return v in (0, "0", "—")

            l1 = [f"📤 Carga: <b>{r['carga']}</b>"]
            if not _oculto(r['entrada']):
                l1.insert(0, f"📥 Entrada: <b>{r['entrada']}</b>")
            if not _oculto(r['rep']):
                l1.append(f"🔁 Repetidos: <b>{r['rep']}</b>")

            l2 = []
            if not _oculto(r['bloq']):
                l2.append(f"{_etiqueta_bloqueados(r['tipo'])}: <b>{r['bloq']}</b>")
            if not _oculto(r['agendas']):
                l2.append(f"📅 Agendas: <b>{r['agendas']}</b>")
            if not _oculto(r['excl']):
                l2.append(f"{_etiqueta_excluidos(r['tipo'])}: <b>{r['excl']}</b>")

            bloque = f"✅ <b>{r['tipo']}</b><br>&nbsp;&nbsp;&nbsp;&nbsp;" + "&nbsp;&nbsp;".join(l1)
            if l2:
                bloque += "<br>&nbsp;&nbsp;&nbsp;&nbsp;" + "&nbsp;&nbsp;".join(l2)
            lineas.append(bloque)

    _teams(
        titulo  = f"{'❌ Errores en' if hubo_error else '✅ Completado'} grupo {horario}",
        mensaje = "<br><br>".join(lineas),
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
    from app.core.postgres import get_config_usuario
    cfg = get_config_global()
    # Misma cuenta que usa el procesamiento real más abajo (ver override en
    # config_global["usuario_watcher_rutas"]), para que el preview de Teams
    # muestre las rutas que efectivamente se van a usar.
    USUARIO_WATCHER = cfg.get("usuario_watcher_rutas", "").strip() or "jorge.gomez"
    MESES = {
        1:"01-Enero", 2:"02-Febrero", 3:"03-Marzo", 4:"04-Abril",
        5:"05-Mayo", 6:"06-Junio", 7:"07-Julio", 8:"08-Agosto",
        9:"09-Septiembre", 10:"10-Octubre", 11:"11-Noviembre", 12:"12-Diciembre",
    }
    import os
    from datetime import date as _date

    def _preview_destinos(tipo: str) -> str:
        """Arma string con las rutas donde se guardarán los archivos."""
        u_cfg = get_config_usuario(USUARIO_WATCHER).get(tipo, {})
        hoy   = _date.today()
        lineas_d = []
        ruta_c = cfg.get(f"ruta_{tipo.lower()}_compartida", "")
        if u_cfg.get("guardar_compartida", True) and ruta_c:
            ruta_full = os.path.join(ruta_c, str(hoy.year), MESES[hoy.month], f"{hoy.day:02d}")
            lineas_d.append(f"&nbsp;&nbsp;📁 Compartida: `{ruta_full}`")
        if u_cfg.get("guardar_local") and u_cfg.get("ruta_local"):
            lineas_d.append(f"&nbsp;&nbsp;💻 Local: `{u_cfg['ruta_local']}`")
        return "  \n".join(lineas_d) if lineas_d else "&nbsp;&nbsp;📁 /tmp (sin rutas configuradas)"

    lineas_deteccion = "<br>".join(
        f"📁 <b>{horario_g}</b> · {', '.join(a.tipo for a in archivos_g)}"
        for horario_g, archivos_g in grupos.items()
    )
    _teams(
        titulo  = f"📂 {len(nuevos)} archivo(s) nuevo(s)",
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