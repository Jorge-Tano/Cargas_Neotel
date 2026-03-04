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
    """
    Context manager para ejecutar queries en PostgreSQL.
    """
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
    """
    Crea las tablas si no existen.
    Ejecutar una sola vez al iniciar el proyecto.
    """
    with postgres_cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS blacklist_gerencial (
                id              SERIAL PRIMARY KEY,
                rut             VARCHAR(20),
                dv              VARCHAR(2),
                nombre          VARCHAR(200),
                cargo           VARCHAR(200),
                fono1           VARCHAR(20),
                fono2           VARCHAR(20),
                fono3           VARCHAR(20),
                fecha_ingreso   DATE NOT NULL DEFAULT CURRENT_DATE,
                activo          BOOLEAN NOT NULL DEFAULT TRUE
            );

            CREATE TABLE IF NOT EXISTS log_procesos (
                id               SERIAL PRIMARY KEY,
                tipo_caso        VARCHAR(50) NOT NULL,
                fecha_proceso    TIMESTAMP NOT NULL DEFAULT NOW(),
                total_entrada    INT DEFAULT 0,
                total_repetidos  INT DEFAULT 0,
                total_bloqueados INT DEFAULT 0,
                total_carga      INT DEFAULT 0,
                archivo_origen   VARCHAR(200)
            );
        """)
    print("✅ Tablas creadas correctamente en PostgreSQL.")


# ─────────────────────────────────────────────
# GESTIÓN LISTA NEGRA
# ─────────────────────────────────────────────

def get_lista_negra() -> set:
    """
    Retorna un set con todos los fonos activos de blacklist_gerencial.
    Combina fono1, fono2 y fono3 en un solo set.
    """
    with postgres_cursor() as cursor:
        cursor.execute("SELECT fono1, fono2, fono3 FROM blacklist_gerencial WHERE activo = TRUE")
        rows = cursor.fetchall()

    fonos = set()
    for row in rows:
        for fono in row:
            if fono and str(fono).strip() not in ("", "None"):
                f = str(fono).strip()
                # Normalizar: quitar 0 inicial para el cruce
                fonos.add(f[1:] if f.startswith("0") else f)
    return fonos


def actualizar_lista_negra(registros: list[dict]) -> dict:
    """
    Sincroniza blacklist_gerencial con PostgreSQL.
    Cada registro es: {nombre, cargo, fono1, fono2?, fono3?}

    Retorna:
    {
        "insertados": int,
        "total": int
    }
    """
    hoy = date.today()
    with postgres_cursor() as cursor:
        # Limpiar e reinsertar (la blacklist gerencial se reemplaza completa)
        cursor.execute("UPDATE blacklist_gerencial SET activo = FALSE")
        if registros:
            execute_values(
                cursor,
                """
                INSERT INTO blacklist_gerencial (nombre, cargo, fono1, fono2, fono3, fecha_ingreso, activo)
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                [
                    (
                        r.get("nombre", ""),
                        r.get("cargo", ""),
                        r.get("fono1", ""),
                        r.get("fono2", ""),
                        r.get("fono3", ""),
                        hoy,
                        True,
                    )
                    for r in registros
                ]
            )

    return {
        "insertados": len(registros),
        "total": len(registros),
    }


def registrar_log(
    tipo_caso: str,
    total_entrada: int,
    total_repetidos: int,
    total_bloqueados: int,
    total_carga: int,
    archivo_origen: str = "",
):
    """
    Registra en log_procesos el resultado de cada proceso diario.
    """
    with postgres_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO log_procesos
                (tipo_caso, total_entrada, total_repetidos, total_bloqueados, total_carga, archivo_origen)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (tipo_caso, total_entrada, total_repetidos, total_bloqueados, total_carga, archivo_origen),
        )


def get_logs(limit: int = 50) -> list:
    """Retorna los ultimos registros del log de procesos."""
    with postgres_cursor() as cursor:
        cursor.execute("""
            SELECT id, tipo_caso, fecha_proceso, total_entrada,
                   total_repetidos, total_bloqueados, total_carga, archivo_origen
            FROM log_procesos
            ORDER BY fecha_proceso DESC
            LIMIT %s
        """, (limit,))
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

def get_total_lista_negra() -> dict:
    """Retorna el total de personas activas en blacklist_gerencial."""
    with postgres_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM blacklist_gerencial WHERE activo = TRUE")
        total = cursor.fetchone()[0]
    return {"personas": total}