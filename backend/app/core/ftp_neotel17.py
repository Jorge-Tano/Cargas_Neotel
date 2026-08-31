"""
ftp_neotel17.py
===============
Conexión FTP (no SFTP) al servidor "Neotel 17" (192.168.10.17), usado
exclusivamente para navegar/descargar los TXT de resoluciones desde
/DOWNLOAD/Resultante_PL, /DOWNLOAD/Resultante_SAV, etc.

Es un servidor Microsoft IIS FTP clásico:
  - No soporta el comando MLSD (listado estructurado moderno).
  - El modo pasivo no es alcanzable desde este cliente (timeout en el canal
    de datos) → se fuerza modo activo.
  - El listado (`LIST`) viene en formato DOS: "MM-DD-YY  HH:MMAM  <DIR>|tamaño  nombre"

Credenciales: NEOTEL17_FTP_HOST/PORT/USER/PASSWORD en el .env.
"""

import ftplib
import io
import re
from datetime import datetime

from app.core.config import get_settings

settings = get_settings()

_RE_LISTADO_DOS = re.compile(
    r"^(\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}[AP]M)\s+(<DIR>|\d+)\s+(.+)$"
)


def _conectar_ftp17() -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.connect(host=settings.neotel17_ftp_host, port=settings.neotel17_ftp_port, timeout=30)
    ftp.login(user=settings.neotel17_ftp_user, passwd=settings.neotel17_ftp_password)
    ftp.set_pasv(False)  # el modo pasivo no es alcanzable en este servidor
    return ftp


def listar_directorio_ftp17(ruta: str = "/DOWNLOAD") -> list[dict]:
    """
    Lista el contenido de `ruta` en el FTP Neotel17.
    Retorna [{"nombre", "es_dir", "tamano", "mtime"}] ordenado por mtime desc.
    """
    ftp = _conectar_ftp17()
    try:
        lineas: list[str] = []
        ftp.dir(ruta, lineas.append)

        entradas = []
        for linea in lineas:
            m = _RE_LISTADO_DOS.match(linea.strip())
            if not m:
                continue
            fecha, hora, tamano_raw, nombre = m.groups()
            if nombre in (".", ".."):
                continue
            es_dir = tamano_raw == "<DIR>"
            tamano = 0 if es_dir else int(tamano_raw)
            try:
                mtime = datetime.strptime(f"{fecha} {hora}", "%m-%d-%y %I:%M%p").timestamp()
            except ValueError:
                mtime = 0.0
            entradas.append({
                "nombre": nombre,
                "es_dir": es_dir,
                "tamano": tamano,
                "mtime": mtime,
            })

        entradas.sort(key=lambda e: e["mtime"], reverse=True)
        return entradas
    finally:
        ftp.quit()


def encontrar_txt_reciente_ftp17(tipo: str, ruta: str = "/DOWNLOAD/Resultante_PL") -> str | None:
    """
    Ubica el TXT de resoluciones más reciente para PL o REFI. Ambos viven en
    la misma carpeta; REFI se distingue por el prefijo "RN" en el nombre.
    """
    entradas = listar_directorio_ftp17(ruta)
    archivos = [e for e in entradas if not e["es_dir"]]
    if tipo == "REFI":
        archivos = [a for a in archivos if a["nombre"].upper().startswith("RN")]
    else:
        archivos = [a for a in archivos if not a["nombre"].upper().startswith("RN")]
    if not archivos:
        return None
    archivos.sort(key=lambda a: a["mtime"], reverse=True)
    return f"{ruta}/{archivos[0]['nombre']}"


def descargar_archivo_ftp17(ruta_completa: str) -> bytes:
    """Descarga un archivo por ruta completa desde el FTP Neotel17."""
    ftp = _conectar_ftp17()
    try:
        buf = io.BytesIO()
        ftp.retrbinary(f"RETR {ruta_completa}", buf.write)
        buf.seek(0)
        return buf.read()
    finally:
        ftp.quit()


# ─────────────────────────────────────────────────────────────
# Subida del archivo de carga en TXT — confirmado por captura de
# FileZilla: el destino real es este FTP (Neotel17), no el SFTP
# principal, en /UPLOAD/leakage/{TIPO}/.
# ─────────────────────────────────────────────────────────────

def subir_archivo_ftp17(path_local: str, ruta_remota: str) -> str:
    """
    Sube un archivo al FTP Neotel17 a la ruta remota exacta indicada
    (carpeta + nombre de archivo). Si ya existe un archivo con ese nombre,
    lo sobrescribe.
    """
    ftp = _conectar_ftp17()
    try:
        with open(path_local, "rb") as f:
            ftp.storbinary(f"STOR {ruta_remota}", f)
        return ruta_remota
    finally:
        ftp.quit()


def subir_archivo_carga_txt(path_local: str, tipo: str) -> str:
    """
    Sube el TXT de carga (ya generado con exportar_txt_carga) al FTP
    Neotel17:
      - SAV, AV, REFI, PL → /UPLOAD/leakage/{TIPO}/ (carpeta confirmada
        por captura de FileZilla).
      - MKT     → /UPLOAD/MKT/ (no es un caso "leakage", carpeta propia).
      - CARRITO → /UPLOAD/Carrito/ (idem, carpeta propia).
    """
    import os
    nombre = os.path.basename(path_local)
    tipo = tipo.upper()
    carpetas_propias = {"MKT": "MKT", "CARRITO": "Carrito"}
    if tipo in carpetas_propias:
        ruta_remota = f"/UPLOAD/{carpetas_propias[tipo]}/{nombre}"
    else:
        ruta_remota = f"/UPLOAD/leakage/{tipo}/{nombre}"
    ruta_final = subir_archivo_ftp17(path_local, ruta_remota)
    print(f"✅ TXT de carga subido a Neotel17: {ruta_final}")
    return ruta_final