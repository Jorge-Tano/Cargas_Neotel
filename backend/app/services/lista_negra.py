# Lista Negra (Blacklist Gerencial) - Sincronizacion con PostgreSQL
# Formato Excel: RUT | DV | NOMBRE | Cargo | TELEFONO 1 | TELEFONO 2 | TELEFONO 3

import os
import glob
import io
import pandas as pd
from app.core.postgres import actualizar_lista_negra

RUTA_RED = r"\\10.0.1.40\informatica\Neotel\Presto\Bloqueos"


def _normalizar_fono(fono) -> str | None:
    """Normaliza fono: quita 0 inicial, retorna None si inválido."""
    f = str(fono).strip().replace(".0", "")
    if not f or f in ("nan", "None", "0", ""):
        return None
    if f.startswith("0"):
        f = f[1:]
    if not f.isdigit() or len(f) < 7:
        return None
    return f


def _normalizar_rut(rut) -> str | None:
    """Normaliza RUT: quita puntos y guiones."""
    r = str(rut).strip().replace(".", "").replace("-", "").replace(".0", "")
    if not r or r in ("nan", "None", ""):
        return None
    # Quitar dígito verificador si viene pegado (ej: 109332577)
    return r


def encontrar_archivo() -> str | None:
    """Busca el archivo de blacklist más reciente en la ruta de red."""
    try:
        archivos = [
            os.path.join(RUTA_RED, f)
            for f in os.listdir(RUTA_RED)
            if f.upper().startswith("BLACK LIST GERENCIA DE LIDER") and f.upper().endswith(".XLSX")
        ]
        return sorted(archivos, reverse=True)[0] if archivos else None
    except Exception as e:
        raise FileNotFoundError(f"No se pudo acceder a {RUTA_RED}: {e}")


def procesar_lista_negra(
    archivo_bytes: bytes = None,
    nombre_archivo: str = None,
    ruta_archivo: str = None,
) -> dict:
    """
    Lee el archivo de blacklist gerencial y sincroniza con PostgreSQL.

    Formato esperado:
    RUT | DV | NOMBRE | Cargo | TELEFONO 1 | TELEFONO 2 | TELEFONO 3

    Puede recibir:
      - archivo_bytes + nombre_archivo → subido manualmente
      - ruta_archivo → ruta local o de red
      - Sin parámetros → busca automáticamente en la ruta de red
    """
    # ── 1. Obtener fuente ────────────────────────────────────
    if archivo_bytes is not None:
        source = io.BytesIO(archivo_bytes)
        archivo_usado = nombre_archivo or "subido_manualmente"
    elif ruta_archivo is not None:
        source = ruta_archivo
        archivo_usado = ruta_archivo
    else:
        found = encontrar_archivo()
        if found is None:
            raise FileNotFoundError(
                f"No se encontró archivo BLACK LIST GERENCIA DE LIDER*.xlsx en {RUTA_RED}"
            )
        source = found
        archivo_usado = found

    # ── 2. Detectar engine ───────────────────────────────────
    if isinstance(source, io.BytesIO):
        header = source.read(4)
        source.seek(0)
        engine = "openpyxl" if header[:2] == b"PK" else "xlrd"
    else:
        engine = "openpyxl"

    # ── 3. Leer Excel (primera hoja) ─────────────────────────
    try:
        df = pd.read_excel(source, dtype=str, engine=engine)
    except PermissionError:
        raise PermissionError(
            "No se pudo leer el archivo. Verifique que no esté abierto en Excel."
        )
    except Exception:
        if isinstance(source, io.BytesIO):
            source.seek(0)
        alt_engine = "xlrd" if engine == "openpyxl" else "openpyxl"
        try:
            df = pd.read_excel(source, dtype=str, engine=alt_engine)
        except PermissionError:
            raise PermissionError(
                "No se pudo leer el archivo. Verifique que no esté abierto en Excel."
            )

    df.columns = df.columns.str.strip()

    # ── 4. Validar columnas ──────────────────────────────────
    tiene_rut  = "RUT" in df.columns
    tiene_fono = "TELEFONO 1" in df.columns
    if not tiene_rut and not tiene_fono:
        raise ValueError(
            f"Formato no reconocido. Columnas encontradas: {list(df.columns)}. "
            f"Se esperan: RUT, DV, NOMBRE, Cargo, TELEFONO 1, TELEFONO 2, TELEFONO 3"
        )

    # ── 5. Construir registros ───────────────────────────────
    registros = []
    for _, row in df.iterrows():
        rut  = _normalizar_rut(row.get("RUT", ""))
        dv   = str(row.get("DV", "")).strip().replace(".0", "")
        dv   = None if dv in ("nan", "None", "") else dv

        nombre = str(row.get("NOMBRE", "")).strip()
        nombre = None if nombre in ("nan", "None", "") else nombre

        cargo  = str(row.get("Cargo", "")).strip()
        cargo  = None if cargo in ("nan", "None", "") else cargo

        fono1 = _normalizar_fono(row.get("TELEFONO 1", ""))
        fono2 = _normalizar_fono(row.get("TELEFONO 2", ""))
        fono3 = _normalizar_fono(row.get("TELEFONO 3", ""))

        # Saltar filas sin RUT ni teléfono
        if not rut and not fono1:
            continue

        registros.append({
            "rut":    rut,
            "dv":     dv,
            "nombre": nombre,
            "cargo":  cargo,
            "fono1":  fono1,
            "fono2":  fono2,
            "fono3":  fono3,
        })

    if not registros:
        raise ValueError("El archivo no contiene registros válidos.")

    # ── 6. Sincronizar con PostgreSQL ────────────────────────
    resultado: dict = actualizar_lista_negra(registros)

    return {
        "insertados":    resultado["insertados"],
        "actualizados":  resultado["actualizados"],
        "sin_cambios":   resultado["sin_cambios"],
        "eliminados":    resultado["eliminados"],
        "total_activos": resultado["total_activos"],
        "archivo_usado": archivo_usado,
    }