import pyodbc
from contextlib import contextmanager
from app.core.config import get_settings
from app.core.postgres import get_config_valor

settings = get_settings()

# Mapeo caso → clave en config_global
_CASOS_VALIDOS = ["SAV_AV", "AV", "PL", "REFI"]
_KEY_DB  = {"SAV_AV": "DB_SAV_AV", "AV": "DB_AV", "PL": "DB_PL", "REFI": "DB_REFI"}
_KEY_ID  = {"SAV_AV": "IDDATABASE_SAV", "AV": "IDDATABASE_AV", "PL": "IDDATABASE_PL", "REFI": "IDDATABASE_REFI"}


def get_sqlserver_connection(database: str = "master") -> pyodbc.Connection:
    """
    Conexión a SQL Server usando Windows Authentication.
    Servidor principal: settings.sqlserver_host (192.168.10.12).
    Queries a ECRM_* usan linked server hacia 192.168.10.17,2133.
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


def get_iddatabase(caso: str) -> int:
    """Lee IDDATABASE_{caso} desde config_global en PostgreSQL."""
    valor = get_config_valor(_KEY_ID.get(caso, ""))
    if not valor:
        raise ValueError(
            f"IDDATABASE para '{caso}' no configurado. "
            f"Configure en la UI → Configuración → IDs de base de datos."
        )
    return int(valor)


def get_db_name(caso: str) -> str:
    """Lee el nombre de BD (ej: ECRM_0265) desde config_global en PostgreSQL."""
    valor = get_config_valor(_KEY_DB.get(caso, ""))
    if not valor:
        raise ValueError(
            f"Nombre de BD para '{caso}' no configurado. "
            f"Configure en la UI → Configuración → IDs de base de datos."
        )
    return valor


def get_repetidos(caso: str, progress_cb=None) -> set:
    if caso not in _CASOS_VALIDOS:
        raise ValueError(f"Caso '{caso}' no reconocido. Válidos: {_CASOS_VALIDOS}")

    db         = get_db_name(caso)
    iddatabase = get_iddatabase(caso)
    linked     = settings.sqlserver_linked_host

    msg = f"Consultando [{db}] IDDATABASE={iddatabase}"
    print(f"[get_repetidos] {msg}")
    if progress_cb:
        progress_cb(f"Verificando repetidos — {msg}")

    query = f"""
        SELECT a.TXTRUT
        FROM [{linked}].[{db}].[dbo].[CONTACTOS] a
        INNER JOIN [{linked}].[{db}].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
        WHERE b.IDDATABASE = {iddatabase}
    """

    try:
        with sqlserver_cursor("master") as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        total = len(rows)
        print(f"[get_repetidos] {caso}: {total} RUTs encontrados en BD")
        if progress_cb:
            progress_cb(f"Repetidos en BD: {total} RUTs ({db} / ID {iddatabase})")
        return {str(row[0]).strip() for row in rows}
    except Exception as e:
        print(f"[get_repetidos] ERROR en {caso}: {e}")
        if progress_cb:
            progress_cb(f"⚠️ Error consultando repetidos: {e}")
        return set()


def get_contactos_efectivos_5757() -> dict:
    """
    Retorna {rut: telefono_gestionado} desde walmart..Tbl_RepositorioContactosEfectivos5757.
    Equivale al BUSCARV(A2;Hoja1!C:D;2;0) del Excel original.
    """
    query = "SELECT rut, Telefono_Gestionado FROM walmart..Tbl_RepositorioContactosEfectivos5757"
    with sqlserver_cursor("walmart") as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    return {str(row[0]).strip(): str(row[1]).strip() for row in rows}