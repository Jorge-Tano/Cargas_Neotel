import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from datetime import date
from app.core.config import get_settings

settings = get_settings()


def get_postgres_connection() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        client_encoding="UTF8",
        options="-c client_encoding=UTF8",
    )
    return conn


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
        """)
        cursor.execute("ALTER TABLE log_procesos ADD COLUMN IF NOT EXISTS usuario VARCHAR(100);")
    print("✅ Tablas creadas correctamente en PostgreSQL.")


# ─────────────────────────────────────────────
# GESTIÓN BLACKLIST GERENCIAL
# ─────────────────────────────────────────────

def get_lista_negra() -> dict:
    """Retorna {ruts, fonos} para cruzar en los procesos."""
    with postgres_cursor() as cursor:
        cursor.execute("""
            SELECT rut, fono1, fono2, fono3
            FROM blacklist_gerencial
            WHERE activo = TRUE
        """)
        rows = cursor.fetchall()

    ruts  = set()
    fonos = set()
    for rut, fono1, fono2, fono3 in rows:
        if rut and str(rut).strip() not in ("", "nan"):
            ruts.add(str(rut).strip())
        for f in [fono1, fono2, fono3]:
            if f and str(f).strip() not in ("", "nan"):
                fonos.add(str(f).strip())

    return {"ruts": ruts, "fonos": fonos}


def get_total_lista_negra() -> dict:
    """Retorna conteo de personas activas."""
    with postgres_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM blacklist_gerencial WHERE activo = TRUE")
        return {"personas": cursor.fetchone()[0]}


def actualizar_lista_negra(registros: list[dict]) -> dict:
    """
    Sincroniza blacklist_gerencial sin duplicar.
    - UPSERT por RUT (índice parcial WHERE rut IS NOT NULL)
    - Sin RUT: inserta solo si fono1 no existe activo
    - Marca inactivos los RUTs que ya no están en el archivo
    """
    with postgres_cursor() as cursor:
        # RUTs activos actuales
        cursor.execute("SELECT rut FROM blacklist_gerencial WHERE activo = TRUE AND rut IS NOT NULL")
        ruts_actuales = {row[0].strip() for row in cursor.fetchall()}
        ruts_nuevos   = {r["rut"] for r in registros if r.get("rut")}
        ruts_eliminar = ruts_actuales - ruts_nuevos

        hoy         = date.today()
        insertados  = 0
        actualizados = 0
        sin_cambios  = 0

        for r in registros:
            rut = r.get("rut")

            if rut:
                # Verificar si existe para comparar cambios
                cursor.execute("""
                    SELECT dv, nombre, cargo, fono1, fono2, fono3
                    FROM blacklist_gerencial
                    WHERE rut = %s AND activo = TRUE
                """, (rut,))
                existente = cursor.fetchone()

                # UPSERT por índice parcial
                cursor.execute("""
                    INSERT INTO blacklist_gerencial
                        (rut, dv, nombre, cargo, fono1, fono2, fono3, fecha_ingreso, activo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT (rut) WHERE rut IS NOT NULL DO UPDATE SET
                        dv            = EXCLUDED.dv,
                        nombre        = EXCLUDED.nombre,
                        cargo         = EXCLUDED.cargo,
                        fono1         = EXCLUDED.fono1,
                        fono2         = EXCLUDED.fono2,
                        fono3         = EXCLUDED.fono3,
                        fecha_ingreso = EXCLUDED.fecha_ingreso,
                        activo        = TRUE
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
                # Sin RUT: insertar solo si fono1 no existe activo
                fono1 = r.get("fono1")
                if fono1:
                    cursor.execute(
                        "SELECT id FROM blacklist_gerencial WHERE fono1 = %s AND activo = TRUE",
                        (fono1,)
                    )
                    if not cursor.fetchone():
                        cursor.execute("""
                            INSERT INTO blacklist_gerencial
                                (rut, dv, nombre, cargo, fono1, fono2, fono3, fecha_ingreso, activo)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                        """, (None, r.get("dv"), r.get("nombre"), r.get("cargo"),
                              fono1, r.get("fono2"), r.get("fono3"), hoy))
                        insertados += 1
                    else:
                        sin_cambios += 1

        # Marcar inactivos los que ya no están
        eliminados = 0
        if ruts_eliminar:
            cursor.execute(
                "UPDATE blacklist_gerencial SET activo = FALSE WHERE rut = ANY(%s)",
                (list(ruts_eliminar),)
            )
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
    """Guarda los RUTs repetidos detectados en un proceso."""
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
                SELECT id, rut, tipo_caso, fecha
                FROM log_repetidos
                WHERE tipo_caso = %s
                ORDER BY fecha DESC
                LIMIT %s
            """, (tipo_caso, limit))
        else:
            cursor.execute("""
                SELECT id, rut, tipo_caso, fecha
                FROM log_repetidos
                ORDER BY fecha DESC
                LIMIT %s
            """, (limit,))
        cols = [desc[0] for desc in cursor.description]
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
            FROM log_auditoria
            ORDER BY fecha DESC
            LIMIT %s
        """, (limit,))
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ─────────────────────────────────────────────
# LOGS
# ─────────────────────────────────────────────

def registrar_log(
    tipo_caso: str,
    total_entrada: int,
    total_repetidos: int,
    total_bloqueados: int,
    total_carga: int,
    archivo_origen: str = "",
    usuario: str = "",
):
    with postgres_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO log_procesos
                (tipo_caso, total_entrada, total_repetidos, total_bloqueados, total_carga, archivo_origen, usuario)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (tipo_caso, total_entrada, total_repetidos, total_bloqueados, total_carga, archivo_origen, usuario),
        )


def get_logs(limit: int = 50) -> list:
    with postgres_cursor() as cursor:
        cursor.execute("""
            SELECT id, tipo_caso, fecha_proceso, total_entrada,
                   total_repetidos, total_bloqueados, total_carga, archivo_origen, usuario
            FROM log_procesos
            ORDER BY fecha_proceso DESC
            LIMIT %s
        """, (limit,))
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]