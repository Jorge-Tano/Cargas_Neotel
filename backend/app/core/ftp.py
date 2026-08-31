"""
Conexion SFTP con FileZilla usando paramiko.
Credenciales y ruta base vienen del .env.
Palabras clave vienen de PostgreSQL (config_global).

Busqueda automatica de archivos:
  Recorre recursivamente buscando archivos .xlsx/.xls que contengan
  las keywords del caso. Las rutas base son fijas por grupo:
    SAV / AV   → {ftp_base}/LEAKAGE DIGITAL
    REFI / PL  → {ftp_base}/LEAKAGE DIGITAL OP
  - sftp_keyword_global : requerida en todos (ej: LEAKAGE)
  - sftp_keyword_{caso} : especifica del caso (ej: SAV, AV, REFI, PL)
  - sftp_max_depth      : profundidad maxima de busqueda (default: 5)
  El matching de keyword de caso es por palabra completa (word-boundary),
  lo que evita que buscar "AV" matchee archivos que contienen "SAV".

Neotel 17 (FTP separado):
  Host   : 192.168.10.17  (o NEOTEL17_FTP_HOST en .env)
  Puerto : 21             (o NEOTEL17_FTP_PORT en .env)
  Usuario: Client1        (o NEOTEL17_FTP_USER en .env)
  Pass   : neopass        (o NEOTEL17_FTP_PASSWORD en .env)
  Usado exclusivamente para descargar los TXT de resoluciones
  desde /DOWNLOAD/Resultante_SAV/.
"""

import paramiko
import io
import logging
import re
import stat
import time
import ftplib
from datetime import date
from app.core.config import get_settings
from app.core.postgres import get_config_global

logger = logging.getLogger(__name__)



settings = get_settings()


# ─────────────────────────────────────────────────────────────
# FTP principal (SFTP / paramiko) — para archivos .xlsx/.xls
# ─────────────────────────────────────────────────────────────

def _get_sftp_config() -> dict:
    cfg      = get_config_global()
    host     = cfg.get("sftp_host", "").strip() or settings.ftp_host
    port_str = cfg.get("sftp_port", "").strip() or str(settings.ftp_port)
    user     = cfg.get("sftp_user", "").strip() or settings.ftp_user
    password = cfg.get("sftp_password", "").strip() or settings.ftp_password

    if not all([host, port_str, user, password]):
        raise ValueError(
            "Credenciales SFTP incompletas. "
            "Verifique FTP_HOST, FTP_PORT, FTP_USER y FTP_PASSWORD en el .env del servidor."
        )

    try:
        max_depth = int(cfg.get("sftp_max_depth", "5").strip())
    except (ValueError, AttributeError):
        max_depth = 5

    return {
        "host":           host,
        "port":           int(port_str),
        "user":           user,
        "password":       password,
        "keyword_global": cfg.get("sftp_keyword_global", "LEAKAGE").strip() or "LEAKAGE",
        "keyword_SAV":    cfg.get("sftp_keyword_SAV",    "SAV").strip()     or "SAV",
        "keyword_AV":     cfg.get("sftp_keyword_AV",     "AV").strip()      or "AV",
        "keyword_REFI":   cfg.get("sftp_keyword_REFI",   "REFI").strip()    or "REFI",
        "keyword_PL":     cfg.get("sftp_keyword_PL",     "PL").strip()      or "PL",
        "keyword_CARRITO": cfg.get("sftp_keyword_CARRITO", "CARRITO").strip() or "CARRITO",
        "keyword_MKT":     cfg.get("sftp_keyword_MKT",     "MKT").strip()     or "MKT",
        "max_depth":      max_depth,
    }


# Casos que NO requieren la keyword global (ej: "LEAKAGE"). El caso CARRITO
# viene de otro flujo (Carrito Abandonado / Seguros) y su nombre de archivo
# no contiene esa palabra, asi que se busca solo por su propia keyword.
_CASOS_SIN_KEYWORD_GLOBAL = {"CARRITO", "MKT"}


def _kw_global_para(cfg: dict, tipo_upper: str) -> str:
    if tipo_upper in _CASOS_SIN_KEYWORD_GLOBAL:
        return ""
    return cfg["keyword_global"]


def get_sftp_client() -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    cfg = _get_sftp_config()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=cfg["host"], port=cfg["port"],
                username=cfg["user"], password=cfg["password"], timeout=30)
    return ssh, ssh.open_sftp()


def _buscar_archivos_recursivo(
    sftp: paramiko.SFTPClient,
    directorio: str,
    kw_global: str,
    kw_caso: str,
    profundidad_max: int,
    profundidad_actual: int = 0,
) -> list[paramiko.SFTPAttributes]:
    """
    Recorre el FTP desde 'directorio' buscando .xlsx/.xls que coincidan
    con kw_global y kw_caso. Devuelve lista de SFTPAttributes con el
    atributo extra .ruta_completa para saber desde donde descargarlo.
    """
    if profundidad_actual > profundidad_max:
        return []

    resultados = []
    try:
        entradas = sftp.listdir_attr(directorio)
    except IOError as exc:
        logger.warning(
            "No se pudo listar '%s' (se omite ese subárbol de la búsqueda): %s",
            directorio, exc,
        )
        return []

    for entrada in entradas:
        ruta_entrada = f"{directorio}/{entrada.filename}"
        if stat.S_ISDIR(entrada.st_mode or 0):
            resultados.extend(
                _buscar_archivos_recursivo(
                    sftp, ruta_entrada, kw_global, kw_caso,
                    profundidad_max, profundidad_actual + 1,
                )
            )
        else:
            nombre_upper = entrada.filename.upper()
            if not nombre_upper.endswith((".XLSX", ".XLS", ".CSV")):
                continue
            if kw_global and kw_global not in nombre_upper:
                continue
            # Matching de palabra completa usando _ como separador.
            # Evita que buscar "AV" matchee "SAV": en LEAKAGE_SAV_PM el
            # segmento previo a AV es "S", que es alfanumérico → no matchea.
            # En LEAKAGE_AV_PM el segmento previo es "_" → sí matchea.
            patron = r'(?<![A-Z0-9])' + re.escape(kw_caso) + r'(?![A-Z0-9])'
            if re.search(patron, nombre_upper):
                entrada.ruta_completa = ruta_entrada  # type: ignore[attr-defined]
                resultados.append(entrada)

    return resultados


def _get_raiz_sftp(tipo_upper: str) -> str:
    if tipo_upper in ("SAV", "AV"):
        return f"{settings.ftp_base}/LEAKAGE DIGITAL"
    elif tipo_upper in ("REFI", "PL"):
        return f"{settings.ftp_base}/LEAKAGE DIGITAL OP"
    elif tipo_upper == "CARRITO":
        return f"{settings.ftp_base}/{date.today().year}"
    elif tipo_upper == "MKT":
        return f"{settings.ftp_base}/{date.today().year}"
    else:
        raise ValueError(f"Tipo de caso no reconocido: '{tipo_upper}'")


def listar_archivos(tipo: str) -> list[str]:
    cfg        = _get_sftp_config()
    tipo_upper = tipo.upper()
    raiz       = _get_raiz_sftp(tipo_upper)
    kw_global  = _kw_global_para(cfg, tipo_upper)
    kw_caso    = cfg.get(f"keyword_{tipo_upper}", tipo_upper)

    ssh, sftp = get_sftp_client()
    try:
        encontrados = _buscar_archivos_recursivo(
            sftp, raiz, kw_global, kw_caso, cfg["max_depth"]
        )
        return [a.filename for a in encontrados]
    finally:
        sftp.close(); ssh.close()


def descargar_archivo_sftp(tipo: str) -> tuple[bytes, str]:
    cfg        = _get_sftp_config()
    tipo_upper = tipo.upper()
    raiz       = _get_raiz_sftp(tipo_upper)
    kw_global  = _kw_global_para(cfg, tipo_upper)
    kw_caso    = cfg.get(f"keyword_{tipo_upper}", tipo_upper)

    ssh, sftp = get_sftp_client()
    try:
        coincidencias = _buscar_archivos_recursivo(
            sftp, raiz, kw_global, kw_caso, cfg["max_depth"]
        )
        if not coincidencias:
            raise FileNotFoundError(
                f"No se encontro ningun archivo {tipo} bajo '{raiz}' "
                f"con keywords '{kw_global}' y '{kw_caso}'."
            )

        coincidencias.sort(key=lambda a: a.st_mtime or 0, reverse=True)
        mejor    = coincidencias[0]
        nombre   = mejor.filename
        ruta_ftp = mejor.ruta_completa  # type: ignore[attr-defined]

        print(f"Descargando {tipo}: {nombre} desde {ruta_ftp}")
        buf = io.BytesIO()
        sftp.getfo(ruta_ftp, buf)
        buf.seek(0)
        return buf.read(), nombre
    finally:
        sftp.close(); ssh.close()


# ─────────────────────────────────────────────────────────────
# NOTA: la subida del TXT de carga NO va por este SFTP.
# El destino real (confirmado por captura de FileZilla) es el FTP
# Neotel17 → ver app.core.ftp_neotel17.subir_archivo_carga_txt().
# ─────────────────────────────────────────────────────────────


def listar_directorio_sftp(ruta: str = "/archivos") -> list[dict]:
    """
    Lista el contenido de `ruta` en el SFTP principal (navegación genérica
    tipo explorador de carpetas, usada por la carga mensual PL/REFI en
    /archivos/{año}/OP/{mes}).
    Retorna [{"nombre", "es_dir", "tamano", "mtime"}] ordenado por mtime desc.
    """
    ssh, sftp = get_sftp_client()
    try:
        entradas = []
        for entrada in sftp.listdir_attr(ruta):
            entradas.append({
                "nombre": entrada.filename,
                "es_dir": bool(stat.S_ISDIR(entrada.st_mode or 0)),
                "tamano": entrada.st_size or 0,
                "mtime": float(entrada.st_mtime or 0),
            })
        entradas.sort(key=lambda e: e["mtime"], reverse=True)
        return entradas
    finally:
        sftp.close(); ssh.close()


def descargar_archivo_sftp_ruta(ruta_completa: str) -> bytes:
    """Descarga un archivo por ruta completa exacta desde el SFTP principal."""
    ssh, sftp = get_sftp_client()
    try:
        buf = io.BytesIO()
        sftp.getfo(ruta_completa, buf)
        buf.seek(0)
        return buf.read()
    finally:
        sftp.close(); ssh.close()


_MESES_ES_OP = [
    "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE",
]


def _es_carpeta_bimestral(nombre: str) -> bool:
    """"JUNIO-JULIO", "JULIO-AGOSTO", etc. — dos meses en español separados por guión."""
    partes = nombre.upper().split("-")
    return len(partes) == 2 and all(p.strip() in _MESES_ES_OP for p in partes)


def encontrar_excel_mensual_reciente(tipo: str) -> str | None:
    """
    Ubica automáticamente el Excel mensual más reciente para PL o REFI,
    navegando /archivos/{año}/OP/{carpeta bimestral con archivos más nuevos}.
    No asume qué carpeta "debería" estar vigente (los nombres no siguen un
    calendario fijo) — simplemente toma la carpeta bimestral con el archivo
    más reciente, que en la práctica es la que se está usando.
    """
    patron = "PAGO_LIVIANO_ENVIAR" if tipo == "PL" else "CALL_REFI"
    hoy = date.today()

    for año in (hoy.year, hoy.year - 1):
        ruta_op = f"{settings.ftp_base}/{año}/OP"
        try:
            entradas = listar_directorio_sftp(ruta_op)
        except Exception:
            continue

        carpetas = [e for e in entradas if e["es_dir"] and _es_carpeta_bimestral(e["nombre"])]
        if not carpetas:
            continue
        carpetas.sort(key=lambda e: e["mtime"], reverse=True)

        for carpeta in carpetas:
            ruta_carpeta = f"{ruta_op}/{carpeta['nombre']}"
            try:
                archivos = listar_directorio_sftp(ruta_carpeta)
            except Exception:
                continue
            candidatos = [
                a for a in archivos
                if not a["es_dir"] and patron in a["nombre"].upper()
            ]
            if candidatos:
                candidatos.sort(key=lambda a: a["mtime"], reverse=True)
                return f"{ruta_carpeta}/{candidatos[0]['nombre']}"
    return None


def descargar_archivo_sftp_por_nombre(
    tipo: str,
    nombre_archivo: str,
    reintentos: int = 3,
    espera_seg: float = 2.0,
) -> tuple[bytes, str]:
    """
    Descarga un archivo específico por nombre exacto desde el SFTP.

    Reintenta la búsqueda si no aparece de inmediato: un listado recursivo
    puede fallar de forma transitoria en algún subdirectorio (timeout de
    red) y devolver resultados incompletos sin que se note — un solo
    intento no alcanza para concluir que el archivo ya no está.
    """
    cfg        = _get_sftp_config()
    tipo_upper = tipo.upper()
    raiz       = _get_raiz_sftp(tipo_upper)
    kw_global  = _kw_global_para(cfg, tipo_upper)
    kw_caso    = cfg.get(f"keyword_{tipo_upper}", tipo_upper)

    ultimo_error: Exception = FileNotFoundError(
        f"No se encontró '{nombre_archivo}' bajo '{raiz}'."
    )

    for intento in range(1, reintentos + 1):
        ssh, sftp = get_sftp_client()
        try:
            coincidencias = _buscar_archivos_recursivo(
                sftp, raiz, kw_global, kw_caso, cfg["max_depth"]
            )
            exacto = next(
                (a for a in coincidencias if a.filename == nombre_archivo), None
            )
            if exacto:
                ruta_ftp = exacto.ruta_completa  # type: ignore[attr-defined]
                print(f"Descargando {tipo}: {exacto.filename} desde {ruta_ftp}")
                buf = io.BytesIO()
                sftp.getfo(ruta_ftp, buf)
                buf.seek(0)
                return buf.read(), exacto.filename
        except Exception as exc:
            ultimo_error = exc
        finally:
            sftp.close(); ssh.close()

        if intento < reintentos:
            logger.warning(
                "[%s] '%s' no encontrado bajo '%s' (intento %d/%d) — "
                "reintentando en %.0fs por si fue un hipo de red...",
                tipo, nombre_archivo, raiz, intento, reintentos, espera_seg,
            )
            time.sleep(espera_seg)

    raise ultimo_error