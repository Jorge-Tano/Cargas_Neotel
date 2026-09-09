import datetime
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
_CASOS_VALIDOS = ["SAV_AV", "AV", "PL", "REFI", "CARRITO", "MKT", "PERDIDAS", "AMALIA", "OP_PERDIDAS", "OP_WHATSAPP"]
_KEY_DB  = {"SAV_AV": "DB_SAV_AV", "AV": "DB_AV", "PL": "DB_PL", "REFI": "DB_REFI", "CARRITO": "DB_CARRITO", "MKT": "DB_MKT", "PERDIDAS": "DB_PERDIDAS", "AMALIA": "DB_AMALIA", "OP_PERDIDAS": "DB_OP_PERDIDAS", "OP_WHATSAPP": "DB_OP_WHATSAPP"}
_KEY_ID  = {"SAV_AV": "IDDATABASE_SAV", "AV": "IDDATABASE_AV", "PL": "IDDATABASE_PL", "REFI": "IDDATABASE_REFI", "CARRITO": "IDDATABASE_CARRITO", "MKT": "IDDATABASE_MKT", "PERDIDAS": "IDDATABASE_PERDIDAS", "AMALIA": "IDDATABASE_AMALIA", "OP_PERDIDAS": "IDDATABASE_OP_PERDIDAS", "OP_WHATSAPP": "IDDATABASE_OP_WHATSAPP"}

# Columnas de CONTACTOS habilitadas para cruzar repetidos/confirmación
# (allowlist: se interpolan directo en el SQL, así que solo estos nombres
# exactos). CARRITO se cruza por TXTPATENTE (no siempre trae RUT válido
# en el origen); PERDIDAS se cruza por TELTELEFONO1 (no tiene RUT en el
# origen, solo teléfono); el resto por TXTRUT.
_COLUMNAS_CONFIRMACION_VALIDAS = {"TXTRUT", "TXTPATENTE", "TELTELEFONO1"}


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


# MKT y CARRITO no escriben un TXTTIPOBASE distintivo (queda vacío, ver
# _construir_carga en mkt.py/carrito_abandonado.py), así que se detectan
# por un patrón en TXTBASE en su lugar (LIKE, no igualdad exacta: el de
# MKT incluye la fecha, ej. "MKT 20260903").
_PATRON_BASE_ACTUAL = {
    "MKT":     "MKT %",
    "CARRITO": "carrito_abandonado",
}

# SAV_AV/AV/REFI/PL/AMALIA SÍ tienen un catálogo real de campañas (tabla
# `DB` en cada ECRM_XXXX, columnas IDDATABASE/DESCRIPCION/FHALTA/CONTACTOS
# — el mismo catálogo que muestra la UI de administración de Neotel). La
# campaña de carga vigente es siempre "la de mayor IDDATABASE cuyo
# nombre calza con el patrón del caso" — confirmado con datos reales
# (ej. REFI: 94 = "BASE RN LEAKAGE SEPTIEMBRE 2026"). Mucho más confiable
# que inferir por actividad en CONTACTOS: un campaña vieja con agentes
# todavía gestionando su backlog puede tener actividad más reciente que
# una campaña nueva recién creada con pocos registros tocados todavía
# (bug real visto en producción: detectaba Agosto en vez de Septiembre).
_PATRON_NOMBRE_CATALOGO = {
    "SAV_AV": "LEAKAGE",
    "AV":     "LEAKAGE",
    "REFI":   "LEAKAGE",
    "PL":     "LEAKAGE",
    "AMALIA": "BDD AMALIA",   # ECRM_0059 — confirmado con catálogo real (IDs 36-39: "BDD AMALIA JUN/JUL/AGO/SEP 2026")
    "OP_PERDIDAS":  "Perdidas Amalia",  # ECRM_0290 — catálogo real (IDs 91-93,96: "Perdidas Amalia {mes} 2026")
    "OP_WHATSAPP":  "WHTSP",            # ECRM_0290 — catálogo real (IDs 94-95: "WHTSP {mes} 2026") — misma BD, campaña separada
}
_CASOS_CATALOGO_NOMBRE = set(_PATRON_NOMBRE_CATALOGO)


def detectar_iddatabase_por_nombre(
    db: str, contiene: str | None = None, no_contiene: list[str] | None = None
) -> tuple[int, str] | None:
    """
    Busca en la tabla `DB` (catálogo de campañas de Neotel: IDDATABASE,
    DESCRIPCION, FHALTA, CONTACTOS...) la de mayor IDDATABASE cuyo
    nombre calce con los filtros — el mismo criterio con el que un
    humano identifica la base correcta a simple vista en la UI de
    administración de Neotel.

    `contiene`: substring que debe aparecer en DESCRIPCION (ej. "LEAKAGE").
    `no_contiene`: lista de substrings que NO deben aparecer.

    Retorna (iddatabase, descripcion) o None si no hay match o la
    consulta falla.
    """
    condiciones: list[str] = []
    params: list[str] = []
    if contiene:
        condiciones.append("DESCRIPCION LIKE ?")
        params.append(f"%{contiene}%")
    for excluir in (no_contiene or []):
        condiciones.append("DESCRIPCION NOT LIKE ?")
        params.append(f"%{excluir}%")
    where = " AND ".join(condiciones) if condiciones else "1=1"

    try:
        linked = settings.sqlserver_linked_host
        query = f"""
            SELECT TOP 1 IDDATABASE, DESCRIPCION
            FROM [{linked}].[{db}].[dbo].[DB]
            WHERE {where}
            ORDER BY IDDATABASE DESC
        """
        with sqlserver_cursor("master") as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
        return (int(row[0]), row[1]) if row else None
    except Exception as e:
        print(f"[detectar_iddatabase_por_nombre] ERROR en BD {db}: {e}")
        return None


def detectar_base_reciente(caso: str, min_registros: int = 0) -> tuple[int, int] | None:
    """
    Busca, para `caso`, el IDDATABASE de la campaña actualmente vigente.

    Para SAV_AV/AV/REFI/PL consulta el catálogo real de campañas (tabla
    `DB`, ver detectar_iddatabase_por_nombre): la de mayor IDDATABASE
    cuyo nombre contiene "LEAKAGE". Para MKT/CARRITO, que no tienen ese
    catálogo con nombres reconocibles, se sigue infiriendo por patrón en
    TXTBASE de CONTACTOS. `min_registros` filtra grupos chicos (datos de
    prueba o residuales) que no representan una campaña real.

    Retorna (iddatabase, cantidad_registros) o None si no hay match, el
    caso no tiene patrón conocido, o la consulta falla.

    Consulta de bajo nivel: no cachea ni decide nada por sí sola — la
    usa app.core.verificador_iddatabase, que sí controla cuándo y con
    qué frecuencia se llama.
    """
    if caso in _CASOS_CATALOGO_NOMBRE:
        try:
            db = get_db_name(caso)
        except Exception as e:
            print(f"[detectar_base_reciente] ERROR en {caso}: {e}")
            return None
        encontrado = detectar_iddatabase_por_nombre(db, contiene=_PATRON_NOMBRE_CATALOGO[caso])
        if not encontrado:
            return None
        iddatabase, _descripcion = encontrado
        cantidad = _contar_contactos_db(db, iddatabase)
        if cantidad < min_registros:
            return None
        return (iddatabase, cantidad)

    patron_base = _PATRON_BASE_ACTUAL.get(caso)
    if not patron_base:
        return None

    try:
        db     = get_db_name(caso)
        linked = settings.sqlserver_linked_host
        query = f"""
            SELECT TOP 1 b.IDDATABASE, COUNT(*) AS cnt
            FROM [{linked}].[{db}].[dbo].[CONTACTOS] a
            INNER JOIN [{linked}].[{db}].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
            WHERE a.TXTBASE LIKE ?
            GROUP BY b.IDDATABASE
            HAVING COUNT(*) >= ?
            ORDER BY b.IDDATABASE DESC
        """
        with sqlserver_cursor("master") as cursor:
            cursor.execute(query, [patron_base, min_registros])
            row = cursor.fetchone()
        return (int(row[0]), int(row[1])) if row else None
    except Exception as e:
        print(f"[detectar_base_reciente] ERROR en {caso}: {e}")
        return None


def _contar_contactos_db(db: str, iddatabase: int) -> int:
    """Cantidad de contactos cargados en `iddatabase` según la propia tabla DB (columna CONTACTOS)."""
    try:
        linked = settings.sqlserver_linked_host
        with sqlserver_cursor("master") as cursor:
            cursor.execute(
                f"SELECT CONTACTOS FROM [{linked}].[{db}].[dbo].[DB] WHERE IDDATABASE = ?",
                [iddatabase],
            )
            row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


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


def get_repetidos(caso: str, columna: str = "TXTRUT", progress_cb=None) -> set:
    if caso not in _CASOS_VALIDOS:
        raise ValueError(f"Caso '{caso}' no reconocido. Válidos: {_CASOS_VALIDOS}")
    if columna not in _COLUMNAS_CONFIRMACION_VALIDAS:
        raise ValueError(f"Columna '{columna}' no permitida. Válidas: {_COLUMNAS_CONFIRMACION_VALIDAS}")

    try:
        db         = get_db_name(caso)
        iddatabase = get_iddatabase(caso)
        linked     = settings.sqlserver_linked_host

        msg = f"Consultando [{db}] IDDATABASE={iddatabase}"
        print(f"[get_repetidos] {msg}")
        if progress_cb:
            progress_cb(f"Verificando repetidos — {msg}")

        query = f"""
            SELECT a.{columna}
            FROM [{linked}].[{db}].[dbo].[CONTACTOS] a
            INNER JOIN [{linked}].[{db}].[dbo].[DB_CONTACTOS] b ON a.IDINTERNO = b.IDINTERNO
            WHERE b.IDDATABASE = {iddatabase}
        """

        with sqlserver_cursor("master") as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        total = len(rows)
        print(f"[get_repetidos] {caso}: {total} valores ({columna}) encontrados en BD")
        if progress_cb:
            progress_cb(f"Repetidos en BD: {total} ({columna}, {db} / ID {iddatabase})")
        return {str(row[0]).strip() for row in rows}
    except Exception as e:
        print(f"[get_repetidos] ERROR en {caso}: {e}")
        if progress_cb:
            progress_cb(f"⚠️ Error consultando repetidos: {e}")
        return set()


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


# Prefijo real de LOTES.DESCRIPCION en Neotel para cada caso — confirmado
# revisando los scripts de import de Neotel (SAV/REFI) y consultando LOTES
# directo para AV/PL/MKT. CARRITO y MKT comparten base (ECRM_0035), por
# eso conviene filtrar también por prefijo, no solo por fecha. PERDIDAS es
# un caso nuevo (ver perdidas.py / neotel_perdidas_import.sql) — el
# prefijo "CargaLlamadasPerdidas" sigue la misma convención que usaba el
# proceso manual anterior para este caso (LOTES ya tenía años de
# "CargaLlamadasPerdidas{fecha}[AM|PM]" antes de automatizar esto).
_PREFIJO_LOTE = {
    "SAV_AV":   "SAVLEAKAGE_",
    "AV":       "AVLEAKAGE_",
    "REFI":     "REFILEAKAGE_",
    "PL":       "PLLEAKAGE_",
    "CARRITO":  "CARRITO_",
    "MKT":      "MKT_",
    "PERDIDAS": "CargaLlamadasPerdidas",
    "AMALIA":   "LoteBddAmalia",  # sin AM/PM: una vez al día (convención real, confirmada en LOTES históricos)
    "OP_PERDIDAS": "CargaAmaliaOpcionesPago",  # convención real confirmada en LOTES (IDs 249-259)
    "OP_WHATSAPP": "CargaWHTSPOpcionesPago",   # idem
}


def obtener_hora_neotel() -> datetime.datetime | None:
    """Hora real del SQL Server de Neotel (GETDATE()) — se usa como punto
    de referencia para buscar el LOTE que generó nuestro propio disparo,
    sin depender del reloj de la máquina que corre este backend."""
    try:
        with sqlserver_cursor("master") as cursor:
            cursor.execute("SELECT GETDATE()")
            return cursor.fetchone()[0]
    except Exception as e:
        print(f"[obtener_hora_neotel] ERROR: {e}")
        return None


def buscar_lote_reciente(caso: str, desde: datetime.datetime) -> dict | None:
    """
    Busca en LOTES (de la BD de `caso`) el lote más nuevo con el prefijo
    esperado, creado desde `desde` (hora real de Neotel, capturada justo
    antes de disparar ExecuteTask00) en adelante. Pensada para confirmar
    de forma precisa que NUESTRO disparo generó un lote real — a
    diferencia de buscar RUT/Patente en CONTACTOS (que puede dar falsos
    positivos con datos preexistentes no relacionados).

    Retorna {"descripcion": str, "ts": datetime, "registros": int} o None
    si todavía no aparece ningún lote nuevo (el llamador decide si
    reintentar). No lanza excepción.
    """
    if caso not in _PREFIJO_LOTE:
        return None
    prefijo = _PREFIJO_LOTE[caso]

    try:
        db     = get_db_name(caso)
        linked = settings.sqlserver_linked_host
        with sqlserver_cursor("master") as cursor:
            cursor.execute(f"""
                SELECT TOP 1 DESCRIPCION, TS, REGISTROS
                FROM [{linked}].[{db}].[dbo].[LOTES]
                WHERE DESCRIPCION LIKE ? AND TS >= ?
                ORDER BY TS DESC
            """, [f"{prefijo}%", desde])
            row = cursor.fetchone()
            if not row:
                return None
            return {"descripcion": row[0], "ts": row[1], "registros": row[2]}
    except Exception as e:
        print(f"[buscar_lote_reciente] ERROR en {caso}: {e}")
        return None


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