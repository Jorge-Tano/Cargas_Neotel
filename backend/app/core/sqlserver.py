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


# Queries usando linked server desde 192.168.10.12 hacia 192.168.10.17,2133
QUERIES_REPETIDOS = {
    "SAV_AV": """
        SELECT a.TXTRUT
        FROM [192.168.10.17,2133].[ECRM_0265].[dbo].[CONTACTOS] a
        INNER JOIN [192.168.10.17,2133].[ECRM_0265].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
        WHERE b.IDDATABASE = 216
    """,
    "AV": """
        SELECT a.TXTRUT
        FROM [192.168.10.17,2133].[ECRM_0250].[dbo].[CONTACTOS] a
        INNER JOIN [192.168.10.17,2133].[ECRM_0250].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
        WHERE b.IDDATABASE = 90
    """,
    "PL": """
        SELECT a.TXTRUT
        FROM [192.168.10.17,2133].[ECRM_0001].[dbo].[CONTACTOS] a
        INNER JOIN [192.168.10.17,2133].[ECRM_0001].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
        WHERE b.IDDATABASE = 131
    """,
    "REFI": """
        SELECT a.TXTRUT
        FROM [192.168.10.17,2133].[ECRM_0289].[dbo].[CONTACTOS] a
        INNER JOIN [192.168.10.17,2133].[ECRM_0289].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
        WHERE b.IDDATABASE = 70
    """,
}


def get_repetidos(caso: str) -> set:
    """
    Retorna un set de RUTs repetidos segun el caso.
    Casos validos: 'SAV_AV', 'AV', 'PL', 'REFI'
    """
    if caso not in QUERIES_REPETIDOS:
        raise ValueError(f"Caso '{caso}' no reconocido. Validos: {list(QUERIES_REPETIDOS.keys())}")

    with sqlserver_cursor("master") as cursor:
        cursor.execute(QUERIES_REPETIDOS[caso])
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