"""
Caso: Llamadas Perdidas
─────────────────────────────────────────────
Entrada: perdidas_{fecha}.xlsx
  - Columna clave: /Teléfono

Proceso:
  1. Leer archivo
  2. Extraer columna de teléfono (/Teléfono)
  3. Aplicar regla del 0 (agregar_cero)
  4. Generar archivo CargaLlamadasPerdidas{fecha}.xlsx

Salida: CargaLlamadasPerdidas{fecha}.xlsx
  Columnas: Telefono1, FechaCarga, FechaLlamado
"""

import pandas as pd
import io
from datetime import date, datetime
from app.services.utils import agregar_cero, exportar_excel
from app.core.postgres import registrar_log
from app.services.utils import agregar_cero, exportar_excel, leer_archivo


COLUMNAS_SALIDA = [
    "Rut", "Digito", "Nombre Cliente", "Apellido Paterno", "Apellido Materno",
    "DISPONIBLE_SA", "Telefono1", "Telefono2", "Telefono3", "Telefono4",
    "Telefono5", "Telefono6", "Producto", "FechaCarga", "FechaLlamado",
    "Estado", "DETALLEOFERTA", "ORDENDISCADO"
]


def procesar_llamadas_perdidas(
    archivo_bytes: bytes,
    nombre_archivo: str,
    output_dir: str = "/tmp"
) -> dict:
    """
    Procesa el archivo de llamadas perdidas y genera el archivo de carga.

    Parámetros:
        archivo_bytes: contenido del archivo Excel en bytes
        nombre_archivo: nombre original del archivo
        output_dir: directorio donde se guardan los archivos generados

    Retorna:
        {
            "archivo_carga": str (path),
            "total_entrada": int,
            "total_carga": int,
            "fecha": str
        }
    """
    hoy = date.today().strftime("%Y%m%d")
    fecha_carga = date.today().strftime("%d/%m/%Y")

    # 1. Leer archivo de entrada (soporta xls, xlsx, xlsm, csv)
    df = leer_archivo(archivo_bytes, nombre_archivo)

    # Limpiar nombre de columnas (pueden venir con encoding raro)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)

    # 2. Identificar columna de teléfono (puede venir como "/Teléfono" o "/TelÃ©fono")
    col_telefono = None
    for col in df.columns:
        if "tel" in col.lower() or "fono" in col.lower():
            col_telefono = col
            break

    if col_telefono is None:
        raise ValueError("No se encontró columna de teléfono en el archivo de entrada.")

    # 3. Construir DataFrame de salida
    df_salida = pd.DataFrame(columns=COLUMNAS_SALIDA)

    # Extraer campos disponibles
    df_salida["Telefono1"] = df[col_telefono].apply(agregar_cero)
    df_salida["FechaCarga"] = fecha_carga

    # FechaLlamado desde columna "Inicio" si existe
    if "Inicio" in df.columns:
        df_salida["FechaLlamado"] = df["Inicio"]

    # Rellenar columnas vacías con string vacío
    df_salida = df_salida.fillna("")

    # 4. Exportar
    nombre_salida = f"CargaLlamadasPerdidas{hoy}.xls"
    path_salida = f"{output_dir}/{nombre_salida}"
    exportar_excel(df_salida, path_salida)

    # 5. Registrar log
    registrar_log(
        tipo_caso="PERDIDAS",
        total_entrada=total_entrada,
        total_repetidos=0,
        total_bloqueados=0,
        total_carga=len(df_salida),
        archivo_origen=nombre_archivo,
    )

    return {
        "archivo_carga": path_salida,
        "total_entrada": total_entrada,
        "total_carga": len(df_salida),
        "fecha": hoy,
    }