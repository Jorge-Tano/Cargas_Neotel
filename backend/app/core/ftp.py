"""
Conexión SFTP con FileZilla usando paramiko.
El servidor usa protocolo SFTP - SSH File Transfer Protocol.
"""

import paramiko
import io
from app.core.config import get_settings

settings = get_settings()


def get_sftp_client() -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    """
    Retorna (ssh_client, sftp_client) conectados al servidor.
    Recuerda cerrar ambos cuando termines:
        sftp.close()
        ssh.close()
    """
    ssh = paramiko.SSHClient()
    # Acepta automáticamente el host key (equivale a "confiar" en el servidor)
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=settings.ftp_host,
        port=settings.ftp_port,
        username=settings.ftp_user,
        password=settings.ftp_password,
        timeout=30,
    )
    sftp = ssh.open_sftp()
    return ssh, sftp


def listar_archivos(path: str = None) -> list[str]:
    """Lista los archivos en la ruta del servidor SFTP."""
    ruta = path or settings.ftp_path
    ssh, sftp = get_sftp_client()
    try:
        archivos = sftp.listdir(ruta)
        return archivos
    finally:
        sftp.close()
        ssh.close()


def descargar_archivo(nombre_archivo: str, path: str = None) -> bytes:
    """
    Descarga un archivo del SFTP y lo retorna como bytes.
    Uso:
        data = descargar_archivo("LEAKAGE_2CALL_REFI_2026-02-26_AM.xlsx")
        df = pd.read_excel(io.BytesIO(data))
    """
    ruta = path or settings.ftp_path
    ruta_completa = f"{ruta}/{nombre_archivo}"

    ssh, sftp = get_sftp_client()
    try:
        buffer = io.BytesIO()
        sftp.getfo(ruta_completa, buffer)
        buffer.seek(0)
        return buffer.read()
    finally:
        sftp.close()
        ssh.close()


def buscar_archivo_por_patron(patron: str, path: str = None) -> str | None:
    """
    Busca el primer archivo en el SFTP que contenga el patrón dado.
    Ejemplo: buscar_archivo_por_patron("LEAKAGE_2CALL_REFI")
    """
    archivos = listar_archivos(path)
    for archivo in archivos:
        if patron.upper() in archivo.upper():
            return archivo
    return None


def descargar_archivo_sftp(tipo: str) -> tuple[bytes, str]:
    """
    Busca y descarga el archivo más reciente del tipo indicado desde el SFTP.
    tipo: 'REFI' o 'PL'
    Retorna (bytes, nombre_archivo)
    """
    from datetime import date

    patrones = {
        "REFI": "LEAKAGE_2CALL_REFI",
        "PL":   "LEAKAGE_2CALL_PL",
    }

    patron = patrones.get(tipo.upper())
    if not patron:
        raise ValueError(f"Tipo '{tipo}' no reconocido. Válidos: REFI, PL")

    archivos = listar_archivos()
    # Filtrar por patrón y ordenar → el último es el más reciente
    coincidencias = sorted([f for f in archivos if patron in f.upper()])

    if not coincidencias:
        raise FileNotFoundError(f"No se encontró ningún archivo {tipo} en el SFTP.")

    nombre = coincidencias[-1]  # El más reciente alfabéticamente (por fecha en nombre)
    data = descargar_archivo(nombre)
    return data, nombre