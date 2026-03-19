"""
Conexion SFTP con FileZilla usando paramiko.
Credenciales y ruta base vienen del .env.
Palabras clave vienen de PostgreSQL (config_global).

Estructura de rutas (automatica): {ftp_base}/{año}/{caso}/{MES}
Filtros por archivo:
  - sftp_keyword_global : requerida en todos (ej: LEAKAGE)
  - sftp_keyword_{caso} : especifica del caso (ej: SAV, AV, REFI, PL)
"""

import paramiko
import io
from datetime import date
from app.core.config import get_settings
from app.core.postgres import get_config_global

settings = get_settings()

MESES_SFTP = {
    1: "ENERO",      2: "FEBRERO",   3: "MARZO",     4: "ABRIL",
    5: "MAYO",       6: "JUNIO",     7: "JULIO",     8: "AGOSTO",
    9: "SEPTIEMBRE", 10: "OCTUBRE",  11: "NOVIEMBRE", 12: "DICIEMBRE",
}


def _build_path_sftp(tipo: str) -> str:
    hoy = date.today()
    tipo_upper = tipo.upper()
    anio = hoy.year
    mes  = MESES_SFTP[hoy.month]
    if tipo_upper == "SAV":
        return f"{settings.ftp_base}/{anio}/SAV/{mes}/LEAKAGE"
    if tipo_upper == "AV":
        return f"{settings.ftp_base}/{anio}/AV/LEAKAGE/{mes}"
    if tipo_upper in ("REFI", "PL"):
        return f"{settings.ftp_base}/{anio}/OP/leakage"
    return f"{settings.ftp_base}/{anio}/{tipo_upper}/{mes}"


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

    return {
        "host":     host,
        "port":     int(port_str),
        "user":     user,
        "password": password,
        "keyword_global": cfg.get("sftp_keyword_global", "LEAKAGE").strip() or "LEAKAGE",
        "keyword_SAV":    cfg.get("sftp_keyword_SAV",    "SAV").strip()     or "SAV",
        "keyword_AV":     cfg.get("sftp_keyword_AV",     "AV").strip()      or "AV",
        "keyword_REFI":   cfg.get("sftp_keyword_REFI",   "REFI").strip()    or "REFI",
        "keyword_PL":     cfg.get("sftp_keyword_PL",     "PL").strip()      or "PL",
    }


def get_sftp_client() -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    cfg = _get_sftp_config()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=cfg["host"], port=cfg["port"],
                username=cfg["user"], password=cfg["password"], timeout=30)
    return ssh, ssh.open_sftp()


def listar_archivos(tipo: str) -> list[str]:
    ruta = _build_path_sftp(tipo)
    ssh, sftp = get_sftp_client()
    try:
        return sftp.listdir(ruta)
    finally:
        sftp.close(); ssh.close()


def descargar_archivo_sftp(tipo: str) -> tuple[bytes, str]:
    cfg        = _get_sftp_config()
    tipo_upper = tipo.upper()
    ruta       = _build_path_sftp(tipo)
    kw_global  = cfg["keyword_global"]
    kw_caso    = cfg.get(f"keyword_{tipo_upper}", tipo_upper)

    ssh, sftp = get_sftp_client()
    try:
        attrs = sftp.listdir_attr(ruta)
        coincidencias = [
            a for a in attrs
            if kw_global in a.filename.upper()
            and kw_caso   in a.filename.upper()
            and a.filename.upper().endswith((".XLSX", ".XLS"))
        ]
        if not coincidencias:
            todos = [a.filename for a in attrs]
            raise FileNotFoundError(
                f"No se encontro ningun archivo {tipo} en '{ruta}'.\n"
                f"Archivos disponibles: {todos}"
            )
        coincidencias.sort(key=lambda a: a.st_mtime, reverse=True)
        nombre = coincidencias[0].filename
        print(f"Descargando {tipo}: {nombre} desde {ruta}")
        buf = io.BytesIO()
        sftp.getfo(f"{ruta}/{nombre}", buf)
        buf.seek(0)
        return buf.read(), nombre
    finally:
        sftp.close(); ssh.close()