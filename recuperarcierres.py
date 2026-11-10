"""
Recupera las grabaciones de CIERRE DE TARJETA de un día y rango de horas.
Junta las dos fuentes de los .exe originales:
  - Walmart..ServiciosEspeciales   (GrabacionesCierreTarjeta.exe)
  - Walmart..LiderAtencionClientes (GrabacionesLiderAtencionClientes.exe)

Uso (por defecto: hoy, de 00:00 a 15:59):
    python recuperarcierres.py --desde 0 --hasta 15 --simular
    python recuperarcierres.py --desde 0 --hasta 15
    python recuperarcierres.py --fecha 2026-10-27 --desde 23 --hasta 23

--simular  solo muestra qué bajaría, no descarga nada.
Si el archivo RUT-DV.mp3 ya existe en destino con el mismo tamaño, se salta (no duplica).
Requiere: pip install pyodbc
"""
import argparse
import ftplib
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import pyodbc

# ---------- Configuración (misma que los .exe originales) ----------
SQL_SERVER = '2CALL-BD2016'
SQL_DB = 'Ticket'
SQL_USER = 'sa'
SQL_PASS = '1dm3n2c1ll'
FTP_SERVER = '192.168.10.30'
FTP_USER = 'Client1'
FTP_PASS = 'neopass'
FTP_DIR = '/MONITOREO'
BASE_DIR = Path(r'\\192.168.10.60\Lider5757\SERVICIOS ESPECIALES\GRABACIONES\CIERRE DE TARJETA')
TMP_DIR = Path(r'C:\tmp\recuperacion_grabaciones')   # carpeta propia, no se toca C:\tmp
LOG_FILE = Path(r'C:\logs\recuperacion_grabaciones_log.txt')

MESES = ['', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio',
         'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

# Mismos filtros que los .exe, con fecha y rango de horas como parámetros
# y sin el UPDATE (el REPLACE del RUT se hace en el SELECT, no modifica la tabla).
SQL = """
SELECT b.Fecha_ts AS FECHA,
       REPLACE(b.rut,' ','') + '-' + b.dv AS Rut_Dv,
       a.[GRABACION] AS GRABACION,
       'ServiciosEspeciales' AS ORIGEN
FROM ticket..Tbl_Ticket_Neotel30 a
INNER JOIN Walmart..ServiciosEspeciales b ON a.IdLlamada = b.Id_llamada
WHERE a.[Dirección] = 'ENTRANTE'
  AND a.[Sub-Estado] = 'AGENT'
  AND b.Resolucion IN ('TITULAR','ADICIONAL')
  AND b.ServiciosEspeciales = 'Retenciones'
  AND b.CerroTarjeta IN ('RETENIDO','SI')
  AND CONVERT(date, b.Fecha_ts) = CONVERT(date, ?, 112)
  AND DATEPART(HOUR, b.Fecha_ts) BETWEEN ? AND ?

UNION ALL

SELECT b.FECHA_LLAMADO,
       REPLACE(b.rut,' ','') + '-' + b.dv,
       a.[GRABACION],
       'LiderAtencionClientes'
FROM ticket..Tbl_Ticket_Neotel30 a
INNER JOIN Walmart..LiderAtencionClientes b ON a.IdLlamada = b.IdLlamada
WHERE a.[Dirección] = 'ENTRANTE'
  AND a.[Sub-Estado] = 'AGENT'
  AND b.Resolucion IN ('TITULAR','ADICIONAL')
  AND b.TIPIFICACION_1 = 'CIERRE TARJETA'
  AND CONVERT(date, b.FECHA_LLAMADO) = CONVERT(date, ?, 112)
  AND DATEPART(HOUR, b.FECHA_LLAMADO) BETWEEN ? AND ?

ORDER BY 1
"""


def nombre_unico(carpeta: Path, nombre: str) -> str:
    base, ext = os.path.splitext(nombre)
    n, candidato = 1, nombre
    while (carpeta / candidato).exists():
        candidato = f'{base}[{n}]{ext}'
        n += 1
    return candidato


def ya_existe(carpeta: Path, nombre: str, tamano) -> bool:
    """True si RUT-DV.mp3 o RUT-DV[n].mp3 ya está con el mismo tamaño."""
    if tamano is None:
        return False
    base, ext = os.path.splitext(nombre)
    for f in carpeta.glob(f'{base}*{ext}'):
        if f.stem == base or f.stem.startswith(base + '['):
            if f.stat().st_size == tamano:
                return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--fecha', default=datetime.now().strftime('%Y-%m-%d'), help='YYYY-MM-DD (por defecto hoy)')
    ap.add_argument('--desde', type=int, default=0, help='hora inicial 0-23 (incluida)')
    ap.add_argument('--hasta', type=int, default=15, help='hora final 0-23 (incluida)')
    ap.add_argument('--simular', action='store_true', help='solo listar, no descargar')
    args = ap.parse_args()

    fecha = datetime.strptime(args.fecha, '%Y-%m-%d').date()

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()])

    destino = BASE_DIR / f'{fecha.month}. {MESES[fecha.month]}' / str(fecha.day)
    logging.info(f'Fecha {fecha}  horas {args.desde:02d}:00 a {args.hasta:02d}:59  ->  {destino}')

    # 1. Lista de grabaciones
    cs = f'DRIVER={{SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};UID={SQL_USER};PWD={SQL_PASS}'
    with pyodbc.connect(cs) as conn:
        cur = conn.cursor()
        f_txt = fecha.strftime('%Y%m%d')   # texto: el driver antiguo 'SQL Server' no acepta parámetros tipo date
        cur.execute(SQL, f_txt, args.desde, args.hasta, f_txt, args.desde, args.hasta)
        filas = cur.fetchall()
    logging.info(f'{len(filas)} grabaciones encontradas')

    # quitar duplicados por nombre de grabación
    vistos, lista = set(), []
    for f in filas:
        if f.GRABACION and f.GRABACION not in vistos:
            vistos.add(f.GRABACION)
            lista.append(f)

    if args.simular:
        for f in lista:
            print(f'{str(f.FECHA)[:16]}  {f.ORIGEN:<22} {f.GRABACION}  ->  {f.Rut_Dv}.mp3')
        print(f'\nTotal: {len(lista)} (simulación, no se descargó nada)')
        return

    destino.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # 2. Descarga por FTP y mueve a la carpeta de red
    ok = saltados = errores = 0
    ftp = ftplib.FTP(FTP_SERVER, timeout=60)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    ftp.voidcmd('TYPE I')
    try:
        for f in lista:
            nuevo = f'{f.Rut_Dv}.mp3'
            try:
                try:
                    tamano = ftp.size(f.GRABACION)
                except ftplib.error_perm:
                    tamano = None
                if ya_existe(destino, nuevo, tamano):
                    saltados += 1
                    logging.info(f'Ya existe, se salta: {nuevo}')
                    continue
                tmp = TMP_DIR / f.GRABACION
                with open(tmp, 'wb') as fh:
                    ftp.retrbinary(f'RETR {f.GRABACION}', fh.write)
                final = destino / nombre_unico(destino, nuevo)
                shutil.move(str(tmp), str(final))
                ok += 1
                logging.info(f'{f.GRABACION} -> {final.name}')
            except Exception as e:
                errores += 1
                logging.error(f'Error con {f.GRABACION} ({nuevo}): {e}')
    finally:
        ftp.quit()

    logging.info(f'Listo. Descargados: {ok}  Ya existían: {saltados}  Errores: {errores}')


if __name__ == '__main__':
    main()