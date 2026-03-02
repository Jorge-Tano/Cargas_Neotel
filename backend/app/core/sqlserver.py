import pyodbc
from contextlib import contextmanager
from app.core.config import get_settings

settings = get_settings()


def get_sqlserver_connection(database: str = "master") -> pyodbc.Connection:
    """
    Conexion a SQL Server usando Windows Authentication.
    Siempre conecta al servidor principal 192.168.10.12.
    Las queries a ECRM_* usan linked server hacia 192.168.10.17,2133.
    """
    conn_str = (
        f"DRIVER={{{settings.sqlserver_driver}}};"
        f"SERVER={settings.sqlserver_host};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


@contextmanager
def sqlserver_cursor(database: str = "master"):
    """
    Context manager para ejecutar queries en SQL Server.
    """
    conn = get_sqlserver_connection(database)
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


import os as _os
import json as _json

# Ruta al archivo de configuracion
_CONFIG_PATH = _os.path.join(_os.path.dirname(__file__), "..", "..", "config.json")

# BD por defecto (se sobreescriben con config.json)
_DB_INFO = {
    "SAV_AV": {"db": "ECRM_0265", "id": 218},
    "AV":     {"db": "ECRM_0250", "id": 92},
    "PL":     {"db": "ECRM_0001", "id": 131},
    "REFI":   {"db": "ECRM_0289", "id": 70},
}


def _leer_config() -> dict:
    """Lee config.json y retorna los IDDATABASE actuales."""
    try:
        with open(_CONFIG_PATH, "r") as f:
            return _json.load(f)
    except Exception:
        return {}


def get_iddatabase(caso: str) -> int:
    """Retorna el IDDATABASE actual para el caso, leyendo config.json."""
    cfg = _leer_config()
    key = f"IDDATABASE_{caso}"
    if key in cfg:
        return int(cfg[key])
    return _DB_INFO.get(caso, {}).get("id", 0)


def get_repetidos(caso: str) -> set:
    """
    Retorna un set de RUTs repetidos segun el caso.
    Casos validos: 'SAV_AV', 'AV', 'PL', 'REFI'
    Lee el IDDATABASE desde config.json para que sea configurable.
    """
    if caso not in _DB_INFO:
        raise ValueError(f"Caso '{caso}' no reconocido. Validos: {list(_DB_INFO.keys())}")

    db = _DB_INFO[caso]["db"]
    iddatabase = get_iddatabase(caso)

    query = f"""
        SELECT a.TXTRUT
        FROM [192.168.10.17,2133].[{db}].[dbo].[CONTACTOS] a
        INNER JOIN [192.168.10.17,2133].[{db}].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
        WHERE b.IDDATABASE = {iddatabase}
    """

    with sqlserver_cursor("master") as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return {str(row[0]).strip() for row in rows}


def get_contactos_efectivos_5757() -> dict:
    """
    Retorna dict {rut: telefono_gestionado} desde walmart..Tbl_RepositorioContactosEfectivos5757
    Equivale al BUSCARV(A2;Hoja1!C:D;2;0) del Excel original.
    """
    query = "SELECT rut, Telefono_Gestionado FROM walmart..Tbl_RepositorioContactosEfectivos5757"
    with sqlserver_cursor("walmart") as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return {str(row[0]).strip(): str(row[1]).strip() for row in rows}