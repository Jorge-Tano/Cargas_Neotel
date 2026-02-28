# Lista Negra - Sincronizacion diaria con PostgreSQL
# Fuente: 10.0.1.40/informatica/Neotel/Presto/Bloqueos/LISTA_NEGRA_GC_{fecha}_Unica.xlsx
# Hoja: FONOS -> columnas FONO, Solicitante, Fecha

import os
import glob
import pandas as pd
from app.core.postgres import actualizar_lista_negra

RUTA_RED = r"\\10.0.1.40\informatica\Neotel\Presto\Bloqueos"


def normalizar_fono(fono: str) -> str:
    """
    Normaliza el fono para almacenar en BD sin el 0 inicial.
    - "0932007062" → "932007062"
    - "932007062"  → "932007062"
    - "0322115101" → "322115101"
    """
    fono = str(fono).strip().replace(".0", "")
    if fono.startswith("0"):
        fono = fono[1:]
    return fono


def encontrar_archivo_hoy() -> str | None:
    """
    Busca el archivo de lista negra más reciente en la ruta de red.
    """
    patron = os.path.join(RUTA_RED, "LISTA_NEGRA_GC_*.xlsx")
    archivos = sorted(glob.glob(patron), reverse=True)
    return archivos[0] if archivos else None


def procesar_lista_negra(
    archivo_bytes: bytes = None,
    nombre_archivo: str = None,
    ruta_archivo: str = None,
) -> dict:
    """
    Lee el archivo de lista negra y sincroniza con PostgreSQL.

    Puede recibir:
      - archivo_bytes + nombre_archivo → subido manualmente desde la web
      - ruta_archivo → ruta local o de red
      - Sin parámetros → busca automáticamente en la ruta de red
    """
    # ── 1. Obtener fuente del archivo ────────────────────────
    if archivo_bytes is not None:
        import io
        source = io.BytesIO(archivo_bytes)
        archivo_usado = nombre_archivo or "subido_manualmente"
    elif ruta_archivo is not None:
        source = ruta_archivo
        archivo_usado = ruta_archivo
    else:
        found = encontrar_archivo_hoy()
        if found is None:
            raise FileNotFoundError(
                f"No se encontró LISTA_NEGRA_GC_*.xlsx en {RUTA_RED}"
            )
        source = found
        archivo_usado = found

    # ── 2. Leer hoja FONOS ───────────────────────────────────
    # Detectar formato real del archivo (puede ser .xls disfrazado de .xlsx)
    try:
        df = pd.read_excel(source, sheet_name="FONOS", dtype=str, engine="openpyxl")
    except Exception:
        df = pd.read_excel(source, sheet_name="FONOS", dtype=str, engine="xlrd")
    df.columns = df.columns.str.strip()

    if "FONO" not in df.columns:
        raise ValueError(f"No se encontró columna FONO. Columnas disponibles: {list(df.columns)}")

    # ── 3. Construir registros normalizados ──────────────────
    registros = []
    for _, row in df.iterrows():
        fono_raw = str(row.get("FONO", "")).strip().replace(".0", "")
        if not fono_raw or fono_raw == "nan":
            continue

        fono = normalizar_fono(fono_raw)
        if not fono:
            continue

        solicitante = str(row.get("Solicitante", "")).strip()
        solicitante = "" if solicitante == "nan" else solicitante

        fecha = str(row.get("Fecha", "")).strip()
        fecha = "" if fecha == "nan" else fecha

        registros.append({
            "fono": fono,
            "solicitante": solicitante or None,
            "fecha_solicitud": fecha or None,
        })

    if not registros:
        raise ValueError("El archivo no contiene fonos válidos en la hoja FONOS.")

    # ── 4. Sincronizar con PostgreSQL ────────────────────────
    resultado = actualizar_lista_negra(registros)

    return {
        "insertados": len(resultado["insertados"]),
        "eliminados": len(resultado["eliminados"]),
        "sin_cambios": resultado["sin_cambios"],
        "total_archivo": len(registros),
        "archivo_usado": archivo_usado,
    }