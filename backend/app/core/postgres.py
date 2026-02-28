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
            CREATE TABLE IF NOT EXISTS lista_negra (
                fono            VARCHAR(20) PRIMARY KEY,
                solicitante     VARCHAR(200),
                fecha_solicitud VARCHAR(50),
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
    Retorna un set con todos los fonos activos de la lista negra.
    """
    with postgres_cursor() as cursor:
        cursor.execute("SELECT fono FROM lista_negra WHERE activo = TRUE")
        rows = cursor.fetchall()
    return {row[0].strip() for row in rows}


def actualizar_lista_negra(registros: list[dict]) -> dict:
    """
    Sincroniza la lista negra con PostgreSQL.
    Cada registro es: {fono, solicitante, fecha_solicitud}

    - Inserta los que no existen
    - Actualiza solicitante/fecha si ya existe
    - Marca como inactivos los que ya no están en el archivo

    Retorna:
    {
        "insertados": [...],
        "eliminados": [...],
        "sin_cambios": int
    }
    """
    with postgres_cursor() as cursor:
        cursor.execute("SELECT fono FROM lista_negra WHERE activo = TRUE")
        existentes = {row[0].strip() for row in cursor.fetchall()}

        nuevos_fonos = {r["fono"] for r in registros}
        insertar = nuevos_fonos - existentes
        eliminar = existentes - nuevos_fonos
        hoy = date.today()

        # Insertar o actualizar registros
        if registros:
            execute_values(
                cursor,
                """
                INSERT INTO lista_negra (fono, solicitante, fecha_solicitud, fecha_ingreso, activo)
                VALUES %s
                ON CONFLICT (fono) DO UPDATE SET
                    activo = TRUE,
                    solicitante = EXCLUDED.solicitante,
                    fecha_solicitud = EXCLUDED.fecha_solicitud,
                    fecha_ingreso = EXCLUDED.fecha_ingreso
                """,
                [(r["fono"], r.get("solicitante"), r.get("fecha_solicitud"), hoy, True)
                 for r in registros if r["fono"] in insertar or r["fono"] in existentes]
            )


        # Marcar inactivos los eliminados
        if eliminar:
            cursor.execute(
                "UPDATE lista_negra SET activo = FALSE WHERE fono = ANY(%s)",
                (list(eliminar),)
            )


    return {
        "insertados": list(insertar),
        "eliminados": list(eliminar),
        "sin_cambios": len(existentes & nuevos_fonos),
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