"""
Conexion SFTP con FileZilla usando paramiko.
Credenciales y ruta base vienen del .env.
Palabras clave vienen de PostgreSQL (config_global).

Busqueda automatica de archivos:
  Recorre recursivamente desde {ftp_base}/{año} buscando archivos .xlsx/.xls
  que contengan las keywords del caso. No depende de rutas fijas.
  - sftp_keyword_global : requerida en todos (ej: LEAKAGE)
  - sftp_keyword_{caso} : especifica del caso (ej: SAV, AV, REFI, PL)
  - sftp_max_depth      : profundidad maxima de busqueda (default: 5)
"""

import paramiko
import io
import stat
from datetime import date
from app.core.config import get_settings
from app.core.postgres import get_config_global

settings = get_settings()


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
            if (
                kw_global in nombre_upper
                and kw_caso in nombre_upper
                and nombre_upper.endswith((".XLSX", ".XLS"))
            ):
                entrada.ruta_completa = ruta_entrada  # type: ignore[attr-defined]
                resultados.append(entrada)

    return resultados


def listar_archivos(tipo: str) -> list[str]:
    cfg        = _get_sftp_config()
    tipo_upper = tipo.upper()
    raiz       = f"{settings.ftp_base}/{date.today().year}"
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
    raiz       = f"{settings.ftp_base}/{date.today().year}"
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
