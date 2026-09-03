import re
import pyodbc
from contextlib import contextmanager
from app.core.config import get_settings
from app.core.postgres import get_config_valor

settings = get_settings()

# Los nombres de BD (DB_SAV_AV, DB_CARRITO, DB_MKT, etc.) se interpolan
# directo en el SQL como identificador (`[{linked}].[{db}].[dbo]...`),
# así que se validan acá antes de usarse en cualquier query — son
# editables como texto libre desde la UI (Configuración → IDs de base
# de datos), y sin esta validación un valor con corchetes/comillas
# permitiría inyección SQL contra el servidor enlazado de producción.
_RE_DB_NAME_VALIDO = re.compile(r"^[A-Za-z0-9_]+$")

# Mapeo caso → clave en config_global
_CASOS_VALIDOS = ["SAV_AV", "AV", "PL", "REFI", "CARRITO", "MKT"]
_KEY_DB  = {"SAV_AV": "DB_SAV_AV", "AV": "DB_AV", "PL": "DB_PL", "REFI": "DB_REFI", "CARRITO": "DB_CARRITO", "MKT": "DB_MKT"}
_KEY_ID  = {"SAV_AV": "IDDATABASE_SAV", "AV": "IDDATABASE_AV", "PL": "IDDATABASE_PL", "REFI": "IDDATABASE_REFI", "CARRITO": "IDDATABASE_CARRITO", "MKT": "IDDATABASE_MKT"}


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


# TXTTIPOBASE que el sistema ya escribe en CONTACTOS al cargar cada caso
# (ver _construir_carga_* en refi_pl.py / sav_av.py) — usado por
# app.core.verificador_iddatabase para detectar cuándo Neotel crea la
# base nueva del mes (o del día 15, en PL) y actualizar config_global
# automáticamente, SIN consultar SQL Server en cada carga (ver ese
# módulo para el porqué: la detección corre solo cerca de la fecha
# esperada, no en cada llamada de get_iddatabase).
_TIPOBASE_ACTUAL = {
    "SAV_AV": "NORMAL",
    "AV":     "ACTIVO",
    "PL":     "PL Leakage",
    "REFI":   "RN Leakage",
}

# MKT y CARRITO no escriben un TXTTIPOBASE distintivo (queda vacío, ver
# _construir_carga en mkt.py/carrito_abandonado.py), así que se detectan
# por un patrón en TXTBASE en su lugar (LIKE, no igualdad exacta: el de
# MKT incluye la fecha, ej. "MKT 20260903").
_PATRON_BASE_ACTUAL = {
    "MKT":     "MKT %",
    "CARRITO": "carrito_abandonado",
}


def detectar_base_reciente(caso: str, min_registros: int = 0) -> tuple[int, int] | None:
    """
    Busca, para `caso`, el IDDATABASE más nuevo entre los que calzan con
    el patrón esperado — típicamente la campaña actualmente en uso. Para
    SAV_AV/AV/PL/REFI filtra por TXTTIPOBASE (columna que llega siempre
    igual); para MKT/CARRITO, que no tienen TipoBase distintivo, filtra
    por un patrón en TXTBASE en su lugar. `min_registros` filtra grupos
    chicos (datos de prueba o residuales) que no representan una
    campaña real.

    Se ordena por IDDATABASE descendente (Neotel los asigna
    secuencialmente al crear cada campaña nueva — confirmado con datos
    reales: SAV Leakage Marzo=217, Abril=218 ... Septiembre=223), NO por
    actividad más reciente (MAX(TS)): una campaña vieja con agentes
    todavía gestionando su backlog puede tener actividad más reciente
    que una campaña nueva recién cargada con pocos registros tocados
    todavía, lo que llevó a detectar como "actual" la de Agosto (222) en
    vez de la de Septiembre (223) — bug real visto en producción.

    Retorna (iddatabase, cantidad_registros) o None si no hay match, el
    caso no tiene patrón conocido, o la consulta falla.

    Consulta de bajo nivel: no cachea ni decide nada por sí sola — la
    usa app.core.verificador_iddatabase, que sí controla cuándo y con
    qué frecuencia se llama.
    """
    patron_tipobase = _TIPOBASE_ACTUAL.get(caso)
    patron_base      = _PATRON_BASE_ACTUAL.get(caso)
    if not patron_tipobase and not patron_base:
        return None

    if patron_tipobase:
        condicion, valor = "a.TXTTIPOBASE = ?", patron_tipobase
    else:
        condicion, valor = "a.TXTBASE LIKE ?", patron_base

    try:
        db     = get_db_name(caso)
        linked = settings.sqlserver_linked_host
        query = f"""
            SELECT TOP 1 b.IDDATABASE, COUNT(*) AS cnt
            FROM [{linked}].[{db}].[dbo].[CONTACTOS] a
            INNER JOIN [{linked}].[{db}].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
            WHERE {condicion}
            GROUP BY b.IDDATABASE
            HAVING COUNT(*) >= ?
            ORDER BY b.IDDATABASE DESC
        """
        with sqlserver_cursor("master") as cursor:
            cursor.execute(query, [valor, min_registros])
            row = cursor.fetchone()
        return (int(row[0]), int(row[1])) if row else None
    except Exception as e:
        print(f"[detectar_base_reciente] ERROR en {caso}: {e}")
        return None


def get_iddatabase(caso: str) -> int:
    """
    Lee IDDATABASE_{caso} desde config_global en PostgreSQL. Se mantiene
    actualizado automáticamente por app.core.verificador_iddatabase
    (SAV_AV/AV/PL/REFI) — este getter solo lee el valor, no consulta
    SQL Server.
    """
    valor = get_config_valor(_KEY_ID.get(caso, ""))
    if not valor:
        raise ValueError(
            f"IDDATABASE para '{caso}' no configurado. "
            f"Configure en la UI → Configuración → IDs de base de datos."
        )
    return int(valor)


def get_db_name(caso: str) -> str:
    """
    Lee el nombre de BD (ej: ECRM_0265) desde config_global en
    PostgreSQL. Se valida el formato acá (no solo al guardar en la UI)
    porque este valor se interpola directo en el SQL de get_repetidos/
    get_ruts_cargados/detectar_base_reciente — cualquier caller que lo
    use queda protegido, no solo el que pasó por el endpoint de config.
    """
    valor = get_config_valor(_KEY_DB.get(caso, ""))
    if not valor:
        raise ValueError(
            f"Nombre de BD para '{caso}' no configurado. "
            f"Configure en la UI → Configuración → IDs de base de datos."
        )
    if not _RE_DB_NAME_VALIDO.match(valor):
        raise ValueError(
            f"Nombre de BD '{valor}' para '{caso}' tiene caracteres no permitidos "
            f"(solo letras, números y guion bajo). Revise Configuración → IDs de base de datos."
        )
    return valor


def get_repetidos(caso: str, progress_cb=None) -> set:
    if caso not in _CASOS_VALIDOS:
        raise ValueError(f"Caso '{caso}' no reconocido. Válidos: {_CASOS_VALIDOS}")

    try:
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


# Columnas de CONTACTOS habilitadas para confirmar una carga (allowlist:
# se interpolan directo en el SQL, así que solo estos nombres exactos).
# MKT no tiene RUT (su Excel de origen solo trae patente/email/teléfono),
# así que se confirma por TXTPATENTE en vez de TXTRUT.
_COLUMNAS_CONFIRMACION_VALIDAS = {"TXTRUT", "TXTPATENTE"}


def get_ruts_cargados(caso: str, valores: list[str], columna: str = "TXTRUT") -> set[str]:
    """
    Verifica cuáles de los `valores` dados ya están presentes en CONTACTOS
    para el caso indicado — pensada para CONFIRMAR, después de subir el
    TXT por FTP, que un lote conocido y acotado de registros efectivamente
    quedó cargado en Neotel. Por defecto compara por TXTRUT; `columna`
    permite usar otro campo (ej. TXTPATENTE para MKT, que no tiene RUT).

    A diferencia de get_repetidos (que trae TODOS los RUT de la BD para
    el cruce previo a la carga), esta filtra por IN (...) — mucho más
    liviano cuando solo interesa un lote puntual. Se consulta en lotes de
    1000 valores para no exceder el límite práctico de parámetros de SQL
    Server en una cláusula IN.
    """
    if caso not in _CASOS_VALIDOS:
        raise ValueError(f"Caso '{caso}' no reconocido. Válidos: {_CASOS_VALIDOS}")
    if columna not in _COLUMNAS_CONFIRMACION_VALIDAS:
        raise ValueError(f"Columna '{columna}' no permitida. Válidas: {_COLUMNAS_CONFIRMACION_VALIDAS}")

    valores_limpios = [str(v).strip() for v in valores if str(v).strip()]
    if not valores_limpios:
        return set()

    confirmados: set[str] = set()
    TAMANO_LOTE = 1000

    try:
        db         = get_db_name(caso)
        iddatabase = get_iddatabase(caso)
        linked     = settings.sqlserver_linked_host

        with sqlserver_cursor("master") as cursor:
            for i in range(0, len(valores_limpios), TAMANO_LOTE):
                lote = valores_limpios[i:i + TAMANO_LOTE]
                placeholders = ", ".join("?" for _ in lote)
                query = f"""
                    SELECT a.{columna}
                    FROM [{linked}].[{db}].[dbo].[CONTACTOS] a
                    INNER JOIN [{linked}].[{db}].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
                    WHERE b.IDDATABASE = ?
                      AND a.{columna} IN ({placeholders})
                """
                cursor.execute(query, [iddatabase] + lote)
                confirmados.update(str(row[0]).strip() for row in cursor.fetchall())
    except Exception as e:
        print(f"[get_ruts_cargados] ERROR en {caso}: {e}")

    return confirmados


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