import pyodbc
from contextlib import contextmanager
from app.core.config import get_settings
from app.core.postgres import get_config_valor

settings = get_settings()

# Mapeo caso → clave en config_global
_CASOS_VALIDOS = ["SAV_AV", "AV", "PL", "REFI", "CARRITO"]
_KEY_DB  = {"SAV_AV": "DB_SAV_AV", "AV": "DB_AV", "PL": "DB_PL", "REFI": "DB_REFI", "CARRITO": "DB_CARRITO"}
_KEY_ID  = {"SAV_AV": "IDDATABASE_SAV", "AV": "IDDATABASE_AV", "PL": "IDDATABASE_PL", "REFI": "IDDATABASE_REFI", "CARRITO": "IDDATABASE_CARRITO"}


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


# ── Configuración de agendas ────────────────────────────────
# db/IDDATABASE ahora se leen de config_global en PostgreSQL (editable en la UI),
# igual que get_repetidos. Las subcategorías se mantienen fijas por tipo.

_AGENDA_KEY_DB = {
    "SAV":  "DB_AGENDA_SAV",
    "AV":   "DB_AGENDA_AV",
    "PL":   "DB_AGENDA_PL",
    "REFI": "DB_AGENDA_REFI",
}
_AGENDA_KEY_ID = {
    "SAV":  "IDDATABASE_AGENDA_SAV",
    "AV":   "IDDATABASE_AGENDA_AV",
    "PL":   "IDDATABASE_AGENDA_PL",
    "REFI": "IDDATABASE_AGENDA_REFI",
}
_AGENDAS_SUBCATEGORIAS = {
    "SAV":  (45, 47),
    "AV":   (45, 47),
    "PL":   (38, 56, 31, 57),
    "REFI": (38, 56, 31, 57),
}


def get_agenda_iddatabase(tipo: str) -> int:
    """Lee IDDATABASE_AGENDA_{tipo} desde config_global en PostgreSQL."""
    valor = get_config_valor(_AGENDA_KEY_ID.get(tipo, ""))
    if not valor:
        raise ValueError(
            f"IDDATABASE de agenda para '{tipo}' no configurado. "
            f"Configure en la UI → Configuración → IDs de base de datos."
        )
    return int(valor)


def get_agenda_db_name(tipo: str) -> str:
    """Lee el nombre de BD de agenda (ej: ECRM_0002) desde config_global en PostgreSQL."""
    valor = get_config_valor(_AGENDA_KEY_DB.get(tipo, ""))
    if not valor:
        raise ValueError(
            f"Nombre de BD de agenda para '{tipo}' no configurado. "
            f"Configure en la UI → Configuración → IDs de base de datos."
        )
    return valor


def get_ruts_agendados(tipo: str) -> set[str]:
    """
    Retorna el conjunto de RUTs con agendas pendientes para el tipo dado.
    Reemplaza la lectura del TXT desde FTP Neotel17.

    SAV  → DB_AGENDA_SAV  / IDDATABASE_AGENDA_SAV  / subcategorias (45, 47)
    AV   → DB_AGENDA_AV   / IDDATABASE_AGENDA_AV   / subcategorias (45, 47)
    PL   → DB_AGENDA_PL   / IDDATABASE_AGENDA_PL   / subcategorias (38, 56, 31, 57)
    REFI → DB_AGENDA_REFI / IDDATABASE_AGENDA_REFI / subcategorias (38, 56, 31, 57)
    """
    tipo = tipo.upper()

    if tipo not in _AGENDAS_SUBCATEGORIAS:
        raise ValueError(
            f"Tipo '{tipo}' no tiene configuración de agendas. "
            f"Válidos: {list(_AGENDAS_SUBCATEGORIAS)}"
        )

    db     = get_agenda_db_name(tipo)
    iddb   = get_agenda_iddatabase(tipo)
    linked = settings.sqlserver_linked_host
    subs   = ", ".join(str(s) for s in _AGENDAS_SUBCATEGORIAS[tipo])

    query = f"""
        SELECT A.TXTRUT
        FROM [{linked}].[{db}].[dbo].[CONTACTOS] a
        INNER JOIN [{linked}].[{db}].[dbo].[DB_CONTACTOS] b
            ON A.IDINTERNO = b.IDINTERNO
        WHERE b.IDDATABASE = {iddb}
          AND b.subcategoria IN ({subs})
    """

    print(f"[AGENDAS] Consultando tipo={tipo} db={db} iddatabase={iddb}", flush=True)

    with sqlserver_cursor("master") as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    ruts = {str(row[0]).strip() for row in rows if row[0]}
    print(f"[AGENDAS] RUTs agendados: {len(ruts):,}", flush=True)
    return ruts