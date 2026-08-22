"""
Caso: Llamadas Perdidas
─────────────────────────────────────────────
Entrada: perdidas_{fecha}.xlsx
  - Columna clave: /Teléfono

Proceso:
  1. Leer archivo
  2. Extraer columna de teléfono (/Teléfono)
  3. Aplicar regla del 0 (agregar_cero)
  4. Separar registros sin teléfono válido (agregar_cero devuelve "00"
     cuando el dato viene vacío/inválido) a un archivo aparte, para que
     NO queden mezclados en la carga.
  5. Generar archivo CargaLlamadasPerdidas{fecha}.xlsx

Salida:
  - CargaLlamadasPerdidas{fecha}.xlsx      (solo teléfonos válidos)
  - SinTelefonoLlamadasPerdidas{fecha}.xlsx (revisión manual)
  Columnas: Telefono1, FechaCarga, FechaLlamado
"""

import pandas as pd
import io
from datetime import date, datetime
from app.core.postgres import registrar_log
from app.services.utils import agregar_cero, exportar_excel, exportar_multi_destino, leer_archivo


COLUMNAS_SALIDA = [
    "Rut", "Digito", "Nombre Cliente", "Apellido Paterno", "Apellido Materno",
    "DISPONIBLE_SA", "Telefono1", "Telefono2", "Telefono3", "Telefono4",
    "Telefono5", "Telefono6", "Producto", "FechaCarga", "FechaLlamado",
    "Estado", "DETALLEOFERTA", "ORDENDISCADO"
]


def procesar_llamadas_perdidas(
    archivo_bytes: bytes,
    nombre_archivo: str,
    output_dirs: dict = None,
    progress_cb=None,
    usuario: str = "",
) -> dict:
    def emit(step):
        if progress_cb:
            progress_cb(step)

    output_dirs = output_dirs or {}
    hoy = date.today().strftime("%Y%m%d")
    fecha_carga = date.today().strftime("%d/%m/%Y")

    # 1. Leer archivo
    emit("Leyendo archivo")
    df = leer_archivo(archivo_bytes, nombre_archivo)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)

    # 2. Identificar columna de teléfono
    emit("Identificando columnas")
    col_telefono = None
    for col in df.columns:
        if "tel" in col.lower() or "fono" in col.lower():
            col_telefono = col
            break

    if col_telefono is None:
        raise ValueError("No se encontró columna de teléfono en el archivo de entrada.")

    # 3. Construir DataFrame de salida
    emit("Construyendo archivo de carga")
    df_salida = pd.DataFrame(columns=COLUMNAS_SALIDA)
    df_salida["Telefono1"] = df[col_telefono].apply(agregar_cero)
    df_salida["FechaCarga"] = fecha_carga
    if "Inicio" in df.columns:
        df_salida["FechaLlamado"] = df["Inicio"]
    df_salida = df_salida.fillna("")

    # 3b. Separar registros con teléfono inválido/vacío (agregar_cero
    # devuelve "00" cuando no hay un número real). No se cargan: se
    # dejan en un archivo aparte para revisión manual.
    emit("Separando registros con teléfono inválido")
    mask_sin_telefono = df_salida["Telefono1"].astype(str).str.strip() == "00"
    df_sin_telefono = df_salida[mask_sin_telefono].reset_index(drop=True)
    df_salida       = df_salida[~mask_sin_telefono].reset_index(drop=True)

    # 4. Exportar: Carga va a compartida y a local; Sin Teléfono solo a compartida
    emit("Generando archivo Excel")
    nombre_salida       = f"CargaLlamadasPerdidas{hoy}.xls"
    nombre_sin_telefono = f"SinTelefonoLlamadasPerdidas{hoy}.xls"
    tareas = [
        (df_salida,       nombre_salida,       "Contactos", True,  "carga"),
        (df_sin_telefono, nombre_sin_telefono, "Contactos", False, "sin_telefono"),
    ]
    paths = exportar_multi_destino(tareas, output_dirs, claves_local={"carga"})
    path_salida       = paths["carga"]
    path_sin_telefono = paths["sin_telefono"]

    # 5. Registrar log (los "sin teléfono" se registran como bloqueados
    # para que queden visibles en el log de auditoría)
    registrar_log(
        tipo_caso="PERDIDAS",
        total_entrada=total_entrada,
        total_repetidos=0,
        total_bloqueados=len(df_sin_telefono),
        total_carga=len(df_salida),
        archivo_origen=nombre_archivo,
        usuario=usuario,
    )

    return {
        "archivo_carga":        path_salida,
        "archivo_sin_telefono": path_sin_telefono,
        "total_entrada":        total_entrada,
        "total_carga":          len(df_salida),
        "total_sin_telefono":   len(df_sin_telefono),
        "fecha":                hoy,
    }