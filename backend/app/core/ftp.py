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
import re
import stat
import ftplib
from datetime import date
from app.core.config import get_settings
from app.core.postgres import get_config_global



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
        "max_depth":      max_depth,
    }


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
    except IOError:
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
            if not nombre_upper.endswith((".XLSX", ".XLS")):
                continue
            if kw_global not in nombre_upper:
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
    """
    Retorna la ruta base en el SFTP según el grupo del caso:
      SAV / AV   → {ftp_base}/LEAKAGE DIGITAL
      REFI / PL  → {ftp_base}/LEAKAGE DIGITAL OP
    """
    if tipo_upper in ("SAV", "AV"):
        return f"{settings.ftp_base}/LEAKAGE DIGITAL"
    else:  # REFI, PL
        return f"{settings.ftp_base}/LEAKAGE DIGITAL OP"


def listar_archivos(tipo: str) -> list[str]:
    cfg        = _get_sftp_config()
    tipo_upper = tipo.upper()
    raiz       = _get_raiz_sftp(tipo_upper)
    kw_global  = cfg["keyword_global"]
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
    kw_global  = cfg["keyword_global"]
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
# FTP Neotel 17 (ftplib) — exclusivo para TXT de resoluciones
# ─────────────────────────────────────────────────────────────

def _get_neotel17_config() -> dict:
    """
    Credenciales del FTP de Neotel 17.
    Se leen primero desde config_global (BD), con fallback a .env.
    Variables .env: NEOTEL17_FTP_HOST, NEOTEL17_FTP_PORT,
                    NEOTEL17_FTP_USER, NEOTEL17_FTP_PASSWORD
    """
    cfg = get_config_global()

    host     = cfg.get("neotel17_ftp_host", "").strip() or getattr(settings, "neotel17_ftp_host", "192.168.10.17")
    port_str = cfg.get("neotel17_ftp_port", "").strip() or str(getattr(settings, "neotel17_ftp_port", 21))
    user     = cfg.get("neotel17_ftp_user", "").strip() or getattr(settings, "neotel17_ftp_user", "Client1")
    password = cfg.get("neotel17_ftp_password", "").strip() or getattr(settings, "neotel17_ftp_password", "neopass")

    return {
        "host":     host,
        "port":     int(port_str),
        "user":     user,
        "password": password,
    }

def get_neotel17_ftp_client() -> ftplib.FTP:
    """
    Conexion FTP simple a Neotel17.
    """

    cfg = _get_neotel17_config()

    print(f"[Neotel17] Conectando a {cfg['host']}:{cfg['port']}", flush=True)

    ftp = ftplib.FTP(timeout=20)

    ftp.connect(
        host=cfg["host"],
        port=cfg["port"],
    )

    print("[Neotel17] Login...", flush=True)

    ftp.login(
        user=cfg["user"],
        passwd=cfg["password"],
    )

    print("[Neotel17] Login OK", flush=True)

    # MUCHOS FTP viejos fallan con PASV
    ftp.set_pasv(False)

    print("[Neotel17] PASV desactivado", flush=True)

    return ftp

def _buscar_txt_neotel17(
    ftp: ftplib.FTP,
    directorio: str,
    tipo_upper: str,
) -> list[dict]:
    """
    Lista los TXT en 'directorio' del FTP de Neotel 17 que correspondan
    al tipo indicado:
      AV  → nombre empieza con "AVANCE_"
      SAV → nombre NO empieza con "AVANCE_"

    Devuelve lista de dicts: {"nombre": str, "ruta": str, "mtime": float}
    ftplib no da mtime directamente; se usa MDTM si el servidor lo soporta,
    con fallback a 0 (toma el primero de la lista).
    """
    resultados = []
    try:
        entradas = ftp.nlst(directorio)   # lista de rutas completas
    except ftplib.error_perm:
        return []

    for ruta in entradas:
        nombre = ruta.split("/")[-1]
        nombre_up = nombre.upper()
        if not nombre_up.endswith(".TXT"):
            continue

        es_avance = nombre_up.startswith("AVANCE_")
        if tipo_upper == "AV" and not es_avance:
            continue
        if tipo_upper == "SAV" and es_avance:
            continue

        # Intentar obtener fecha de modificación vía MDTM
        mtime = 0.0
        try:
            resp = ftp.sendcmd(f"MDTM {ruta}")   # "213 YYYYMMDDHHMMSS"
            mtime = float(resp[4:].strip())
        except Exception:
            pass

        resultados.append({"nombre": nombre, "ruta": ruta, "mtime": mtime})

    return resultados


def descargar_txt_neotel17(tipo: str) -> tuple[bytes, str]:
    """
    Descarga el TXT de resoluciones desde el FTP de Neotel 17.

    Ruta fija:  /DOWNLOAD/Resultante_SAV/
      SAV → DDMMHHMM.TXT           (sin prefijo AVANCE_)
      AV  → AVANCE_DDMMHHMM.txt.TXT

    Devuelve (contenido_bytes, nombre_archivo).
    Lanza FileNotFoundError si no se encuentra el archivo.
    """
    tipo_upper  = tipo.upper()
    RUTA_FIJA   = "/DOWNLOAD/Resultante_SAV"

    ftp = get_neotel17_ftp_client()
    try:
        print(f"[Neotel17] Buscando TXT tipo={tipo_upper}", flush=True)
        encontrados = _buscar_txt_neotel17(ftp, RUTA_FIJA, tipo_upper)
        print(f"[Neotel17] Encontrados: {len(encontrados)}", flush=True)
        if not encontrados:
            raise FileNotFoundError(
                f"No se encontró ningún TXT de resoluciones para '{tipo}' "
                f"en '{RUTA_FIJA}' del FTP Neotel 17 ({_get_neotel17_config()['host']})."
            )

        # El más reciente primero
        encontrados.sort(key=lambda x: x["mtime"], reverse=True)
        mejor  = encontrados[0]
        nombre = mejor["nombre"]
        ruta   = mejor["ruta"]

        print(f"[Neotel17] Descargando TXT resoluciones {tipo}: {nombre} desde {ruta}", flush=True)
        buf = io.BytesIO()
        ftp.retrbinary(f"RETR {ruta}", buf.write)
        buf.seek(0)
        return buf.read(), nombre
    finally:
        try:
            ftp.quit()
        except Exception:
            pass