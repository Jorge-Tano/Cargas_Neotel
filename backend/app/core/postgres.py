import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from datetime import date
from app.core.config import get_settings

settings = get_settings()


def get_postgres_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        client_encoding="UTF8",
        options="-c client_encoding=UTF8",
    )


@contextmanager
def postgres_cursor():
    conn = get_postgres_connection()
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


# ─────────────────────────────────────────────
# INICIALIZACIÓN DE TABLAS
# ─────────────────────────────────────────────

# Valores iniciales que se insertan UNA SOLA VEZ en la BD.
# Después se gestionan exclusivamente desde la UI → config_global.
# Editar aquí solo si se hace un reset completo de la BD.
_SEED_CONFIG = {
    # IDs de campaña en SQL Server — se actualizan mensualmente desde la UI
    "IDDATABASE_SAV":  "217",
    "IDDATABASE_AV":   "91",
    "IDDATABASE_PL":   "135",
    "IDDATABASE_REFI": "76",
    # Nombres de BD SQL Server
    "DB_SAV_AV": "ECRM_0265",
    "DB_AV":     "ECRM_0250",
    "DB_PL":     "ECRM_0001",
    "DB_REFI":   "ECRM_0289",
    # Rutas de red compartida por proceso
    "ruta_sav_compartida":      "",
    "ruta_av_compartida":       "",
    "ruta_refi_compartida":     "",
    "ruta_pl_compartida":       "",
    "ruta_perdidas_compartida": "",
    # Ruta de red para buscar el archivo de Lista Negra automáticamente
    "ruta_blacklist_red":       "",
    # Flags
    "guardar_compartida": "true",  # legacy — migrado a config_usuario.guardar_compartida
    # SFTP — credenciales y ruta base vienen del .env
    # Todas las rutas: {ftp_base}/{año}/{caso}/{MES}
    "sftp_keyword_global": "LEAKAGE",  # aplica a todos los casos
    "sftp_keyword_SAV":    "SAV",      # editable desde la UI
    "sftp_keyword_AV":     "AV",
    "sftp_keyword_REFI":   "REFI",
    "sftp_keyword_PL":     "PL",
    # LDAP — se leen del .env; se pueden sobrescribir desde la UI si se agrega esa sección
    "ldap_host":  "",
    "ldap_port":  "",
    "ad_domain":  "",
    "ad_base_dn": "",
}


def init_tables():
    with postgres_cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS blacklist_gerencial (
                id            SERIAL PRIMARY KEY,
                rut           VARCHAR(20),
                dv            VARCHAR(2),
                nombre        VARCHAR(200),
                cargo         VARCHAR(200),
                fono1         VARCHAR(20),
                fono2         VARCHAR(20),
                fono3         VARCHAR(20),
                fecha_ingreso DATE NOT NULL DEFAULT CURRENT_DATE,
                activo        BOOLEAN NOT NULL DEFAULT TRUE
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_bg_rut_unique
                ON blacklist_gerencial(rut) WHERE rut IS NOT NULL;

            CREATE INDEX IF NOT EXISTS idx_bg_fono1 ON blacklist_gerencial(fono1);
            CREATE INDEX IF NOT EXISTS idx_bg_fono2 ON blacklist_gerencial(fono2);
            CREATE INDEX IF NOT EXISTS idx_bg_fono3 ON blacklist_gerencial(fono3);

            CREATE TABLE IF NOT EXISTS log_procesos (
                id               SERIAL PRIMARY KEY,
                tipo_caso        VARCHAR(50) NOT NULL,
                fecha_proceso    TIMESTAMP NOT NULL DEFAULT NOW(),
                total_entrada    INT DEFAULT 0,
                total_repetidos  INT DEFAULT 0,
                total_bloqueados INT DEFAULT 0,
                total_carga      INT DEFAULT 0,
                archivo_origen   VARCHAR(200),
                usuario          VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS log_auditoria (
                id       SERIAL PRIMARY KEY,
                fecha    TIMESTAMP NOT NULL DEFAULT NOW(),
                usuario  VARCHAR(100),
                accion   VARCHAR(100) NOT NULL,
                detalle  TEXT
            );

            CREATE TABLE IF NOT EXISTS log_repetidos (
                id         SERIAL PRIMARY KEY,
                rut        VARCHAR(20) NOT NULL,
                tipo_caso  VARCHAR(50) NOT NULL,
                fecha      TIMESTAMP NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_lr_rut       ON log_repetidos(rut);
            CREATE INDEX IF NOT EXISTS idx_lr_tipo_caso ON log_repetidos(tipo_caso);

            CREATE TABLE IF NOT EXISTS config_global (
                clave       VARCHAR(100) PRIMARY KEY,
                valor       TEXT NOT NULL,
                descripcion VARCHAR(200)
            );

            CREATE TABLE IF NOT EXISTS config_usuario (
                usuario            VARCHAR(100) NOT NULL,
                tipo               VARCHAR(20)  NOT NULL,
                ruta_local         TEXT NOT NULL DEFAULT '',
                guardar_local      BOOLEAN NOT NULL DEFAULT FALSE,
                guardar_compartida BOOLEAN NOT NULL DEFAULT TRUE,
                PRIMARY KEY (usuario, tipo)
            );
        """)
        cursor.execute("ALTER TABLE log_procesos ADD COLUMN IF NOT EXISTS usuario VARCHAR(100);")
        cursor.execute("ALTER TABLE config_usuario ADD COLUMN IF NOT EXISTS guardar_compartida BOOLEAN NOT NULL DEFAULT TRUE;")

        # Seed inicial: inserta solo si la clave no existe todavía
        for clave, valor in _SEED_CONFIG.items():
            cursor.execute("""
                INSERT INTO config_global (clave, valor)
                VALUES (%s, %s)
                ON CONFLICT (clave) DO NOTHING
            """, (clave, valor))

    print("✅ Tablas e inicialización correctas en PostgreSQL.")


# ─────────────────────────────────────────────
# GESTIÓN BLACKLIST GERENCIAL
# ─────────────────────────────────────────────

def get_lista_negra() -> dict:
    with postgres_cursor() as cursor:
        cursor.execute("""
            SELECT rut, fono1, fono2, fono3
            FROM blacklist_gerencial
            WHERE activo = TRUE
        """)
        rows = cursor.fetchall()

    ruts, fonos = set(), set()
    for rut, fono1, fono2, fono3 in rows:
        if rut and str(rut).strip() not in ("", "nan"):
            ruts.add(str(rut).strip())
        for f in [fono1, fono2, fono3]:
            if f and str(f).strip() not in ("", "nan"):
                fonos.add(str(f).strip())
    return {"ruts": ruts, "fonos": fonos}


def get_total_lista_negra() -> dict:
    with postgres_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM blacklist_gerencial WHERE activo = TRUE")
        return {"personas": cursor.fetchone()[0]}


def actualizar_lista_negra(registros: list[dict]) -> dict:
    with postgres_cursor() as cursor:
        cursor.execute("SELECT rut FROM blacklist_gerencial WHERE activo = TRUE AND rut IS NOT NULL")
        ruts_actuales = {row[0].strip() for row in cursor.fetchall()}
        ruts_nuevos   = {r["rut"] for r in registros if r.get("rut")}
        ruts_eliminar = ruts_actuales - ruts_nuevos

        hoy = date.today()
        insertados = actualizados = sin_cambios = eliminados = 0

        for r in registros:
            rut = r.get("rut")
            if rut:
                cursor.execute("""
                    SELECT dv, nombre, cargo, fono1, fono2, fono3
                    FROM blacklist_gerencial WHERE rut = %s AND activo = TRUE
                """, (rut,))
                existente = cursor.fetchone()
                cursor.execute("""
                    INSERT INTO blacklist_gerencial
                        (rut, dv, nombre, cargo, fono1, fono2, fono3, fecha_ingreso, activo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT (rut) WHERE rut IS NOT NULL DO UPDATE SET
                        dv=EXCLUDED.dv, nombre=EXCLUDED.nombre, cargo=EXCLUDED.cargo,
                        fono1=EXCLUDED.fono1, fono2=EXCLUDED.fono2, fono3=EXCLUDED.fono3,
                        fecha_ingreso=EXCLUDED.fecha_ingreso, activo=TRUE
                """, (rut, r.get("dv"), r.get("nombre"), r.get("cargo"),
                      r.get("fono1"), r.get("fono2"), r.get("fono3"), hoy))
                if existente is None:
                    insertados += 1
                else:
                    campos_nuevos = (r.get("dv"), r.get("nombre"), r.get("cargo"),
                                     r.get("fono1"), r.get("fono2"), r.get("fono3"))
                    if existente != campos_nuevos:
                        actualizados += 1
                    else:
                        sin_cambios += 1
            else:
                fono1 = r.get("fono1")
                if fono1:
                    cursor.execute(
                        "SELECT id FROM blacklist_gerencial WHERE fono1 = %s AND activo = TRUE",
                        (fono1,)
                    )
                    if not cursor.fetchone():
                        cursor.execute("""
                            INSERT INTO blacklist_gerencial
                                (dv, nombre, cargo, fono1, fono2, fono3, fecha_ingreso, activo)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                        """, (r.get("dv"), r.get("nombre"), r.get("cargo"),
                              fono1, r.get("fono2"), r.get("fono3"), hoy))
                        insertados += 1

        if ruts_eliminar:
            cursor.execute("""
                UPDATE blacklist_gerencial SET activo = FALSE
                WHERE rut = ANY(%s) AND activo = TRUE
            """, (list(ruts_eliminar),))
            eliminados = len(ruts_eliminar)

        cursor.execute("SELECT COUNT(*) FROM blacklist_gerencial WHERE activo = TRUE")
        total_activos = cursor.fetchone()[0]

    return {
        "insertados":    insertados,
        "actualizados":  actualizados,
        "sin_cambios":   sin_cambios,
        "eliminados":    eliminados,
        "total_activos": total_activos,
    }


# ─────────────────────────────────────────────
# REPETIDOS
# ─────────────────────────────────────────────

def registrar_repetidos(ruts: list[str], tipo_caso: str):
    if not ruts:
        return
    with postgres_cursor() as cursor:
        execute_values(
            cursor,
            "INSERT INTO log_repetidos (rut, tipo_caso) VALUES %s",
            [(rut, tipo_caso) for rut in ruts]
        )


def get_repetidos_log(tipo_caso: str = None, limit: int = 200) -> list:
    with postgres_cursor() as cursor:
        if tipo_caso:
            cursor.execute("""
                SELECT id, rut, tipo_caso, fecha FROM log_repetidos
                WHERE tipo_caso = %s ORDER BY fecha DESC LIMIT %s
            """, (tipo_caso, limit))
        else:
            cursor.execute("""
                SELECT id, rut, tipo_caso, fecha FROM log_repetidos
                ORDER BY fecha DESC LIMIT %s
            """, (limit,))
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ─────────────────────────────────────────────
# AUDITORÍA
# ─────────────────────────────────────────────

def registrar_auditoria(usuario: str, accion: str, detalle: str = ""):
    with postgres_cursor() as cursor:
        cursor.execute(
            "INSERT INTO log_auditoria (usuario, accion, detalle) VALUES (%s, %s, %s)",
            (usuario, accion, detalle),
        )


def get_auditoria(limit: int = 100) -> list:
    with postgres_cursor() as cursor:
        cursor.execute("""
            SELECT id, fecha, usuario, accion, detalle
            FROM log_auditoria ORDER BY fecha DESC LIMIT %s
        """, (limit,))
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ─────────────────────────────────────────────
# LOGS
# ─────────────────────────────────────────────

def registrar_log(
    tipo_caso: str, total_entrada: int, total_repetidos: int,
    total_bloqueados: int, total_carga: int,
    archivo_origen: str = "", usuario: str = "",
):
    with postgres_cursor() as cursor:
        cursor.execute("""
            INSERT INTO log_procesos
                (tipo_caso, total_entrada, total_repetidos, total_bloqueados,
                 total_carga, archivo_origen, usuario)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (tipo_caso, total_entrada, total_repetidos, total_bloqueados,
              total_carga, archivo_origen, usuario))


def get_logs(limit: int = 50) -> list:
    with postgres_cursor() as cursor:
        cursor.execute("""
            SELECT id, tipo_caso, fecha_proceso, total_entrada,
                   total_repetidos, total_bloqueados, total_carga, archivo_origen, usuario
            FROM log_procesos ORDER BY fecha_proceso DESC LIMIT %s
        """, (limit,))
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ─────────────────────────────────────────────
# CONFIG GLOBAL
# ─────────────────────────────────────────────

def get_config_global() -> dict:
    """Retorna toda la config global como dict desde la BD."""
    with postgres_cursor() as cursor:
        cursor.execute("SELECT clave, valor FROM config_global")
        return {row[0]: row[1] for row in cursor.fetchall()}


def get_config_valor(clave: str) -> str:
    """Retorna el valor de una clave específica, o '' si no existe."""
    return get_config_global().get(clave, "")


def set_config_global(cambios: dict):
    """Guarda múltiples claves en config_global (upsert)."""
    with postgres_cursor() as cursor:
        for clave, valor in cambios.items():
            cursor.execute("""
                INSERT INTO config_global (clave, valor) VALUES (%s, %s)
                ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
            """, (clave, str(valor)))


# ─────────────────────────────────────────────
# CONFIG USUARIO
# ─────────────────────────────────────────────

TIPOS_CASO = ["SAV", "AV", "REFI", "PL", "PERDIDAS"]


def get_config_usuario(usuario: str) -> dict:
    with postgres_cursor() as cursor:
        cursor.execute("""
            SELECT tipo, ruta_local, guardar_local, guardar_compartida
            FROM config_usuario WHERE usuario = %s
        """, (usuario,))
        rows = cursor.fetchall()
    result = {t: {"ruta_local": "", "guardar_local": False, "guardar_compartida": True} for t in TIPOS_CASO}
    for tipo, ruta, guardar_l, guardar_c in rows:
        result[tipo] = {"ruta_local": ruta, "guardar_local": guardar_l, "guardar_compartida": guardar_c}
    return result


def set_config_usuario(usuario: str, tipo: str, ruta_local: str,
                       guardar_local: bool, guardar_compartida: bool = True):
    with postgres_cursor() as cursor:
        cursor.execute("""
            INSERT INTO config_usuario (usuario, tipo, ruta_local, guardar_local, guardar_compartida)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (usuario, tipo) DO UPDATE SET
                ruta_local         = EXCLUDED.ruta_local,
                guardar_local      = EXCLUDED.guardar_local,
                guardar_compartida = EXCLUDED.guardar_compartida
        """, (usuario, tipo, ruta_local, guardar_local, guardar_compartida))