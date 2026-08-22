"""
mkt.py
======
Procesa el caso "MKT" (Marketing / formulario de captación).

A diferencia de SAV, AV, REFI y PL:
  - El archivo de origen es un XLSX (no CSV), formulario que llega
    TODOS LOS DÍAS con nombre tipo: "Formulario (MKT) DD-MM-AAAA.xlsx".

  - No requiere cruces de repetidos, lista negra ni agendas SQL Server.
    Es una transformación directa: se lee el Excel, se mapea al formato
    de carga y se exporta. Cada corrida es independiente (no se acumulan
    exclusiones entre días), igual que Carrito Abandonado.

Columnas de origen (XLSX):
    created_time, número_patente, email, phone_number

Columnas de salida (carga):
    BASE, RUT, Digito, Nombre Cliente, Apellido Paterno, ApellidoMaterno,
    Tipo, Patente, Marca, Modelo, Año, NMotor, EmailCliente,
    Telefono1..Telefono5, DECILE_RANK, SEXO, Comuna, Ciudad,
    FechaNacimiento, Edad, NACIONALIDAD, ESTADO_CIVIL, HIJOS_GFAM, GSE,
    REGION_NATURAL, TipoBase, Fecha Carga, Orden_Discado, cupo,
    DESCUENTO, INSPECCION, MensajeWSP

Nota sobre teléfonos: llegan en formato internacional "+56XXXXXXXXX"
(12 caracteres). Se reconstruyen al formato local de marcado de 10
dígitos "0XXXXXXXXX", quitando el prefijo "+56" y anteponiendo "0".
"""

import re
import pandas as pd
from datetime import date

from app.services.utils import exportar_excel, exportar_multi_destino, nombre_sin_colision
from app.core.postgres import registrar_log

_RE_TEL_INTL = re.compile(r"^\+?56(\d{9})$")


def _reconstruir_telefono_mkt(telefono_raw: str) -> str:
    """
    "+56966406887" -> "0966406887"
    Si el número ya viene en formato local (9 o 10 dígitos) o con
    cualquier otro formato no reconocido, se deja tal cual — solo se
    corrige el caso "+56" + 9 dígitos.
    """
    t = (telefono_raw or "").strip().replace(" ", "")
    m = _RE_TEL_INTL.match(t)
    if m:
        return "0" + m.group(1)
    if len(t) == 9 and t.isdigit():
        return "0" + t
    return t


def _col(df: pd.DataFrame, col: str, default: str = "") -> list:
    if col in df.columns:
        return df[col].fillna("").astype(str).tolist()
    return [default] * len(df)


def _leer_xlsx_mkt(archivo_bytes: bytes) -> pd.DataFrame:
    import io
    df = pd.read_excel(io.BytesIO(archivo_bytes), dtype=str)
    df.columns = df.columns.str.strip()
    return df


def procesar_mkt(
    archivo_bytes: bytes,
    nombre_archivo: str,
    output_dirs: dict = None,
    progress_cb=None,
    usuario: str = "",
) -> dict:
    """
    Transforma el Excel de MKT al formato de carga.
    No aplica cruces de repetidos, lista negra ni agendas (por diseño).
    """
    def emit(step):
        if progress_cb:
            progress_cb(step)

    output_dirs = output_dirs or {}
    fecha_carga = date.today().strftime("%d-%m-%Y")
    hoy_compacto = date.today().strftime("%Y%m%d")

    # 1. Leer archivo
    emit("Leyendo archivo Excel")
    df = _leer_xlsx_mkt(archivo_bytes)
    total_entrada = len(df)
    n = len(df)

    # 2. Reconstruir teléfonos
    emit("Procesando teléfonos")
    telefonos1 = [_reconstruir_telefono_mkt(v) for v in _col(df, "phone_number")]

    # 3. Construir DataFrame de carga
    emit("Construyendo archivo de carga")
    df_carga = pd.DataFrame({
        "BASE":              [f"MKT {hoy_compacto}"] * n,
        "RUT":               [""] * n,
        "Digito":            [""] * n,
        "Nombre Cliente":    [""] * n,
        "Apellido Paterno":  [""] * n,
        "ApellidoMaterno":   [""] * n,
        "Tipo":              [""] * n,
        "Patente":           _col(df, "número_patente"),
        "Marca":             [""] * n,
        "Modelo":            [""] * n,
        "Año":               [""] * n,
        "NMotor":            [""] * n,
        "EmailCliente":      _col(df, "email"),
        "Telefono1":         telefonos1,
        "Telefono2":         [""] * n,
        "Telefono3":         [""] * n,
        "Telefono4":         [""] * n,
        "Telefono5":         [""] * n,
        "DECILE_RANK":       [""] * n,
        "SEXO":              [""] * n,
        "Comuna":            [""] * n,
        "Ciudad":            [""] * n,
        "FechaNacimiento":   [""] * n,
        "Edad":              [""] * n,
        "NACIONALIDAD":      [""] * n,
        "ESTADO_CIVIL":      [""] * n,
        "HIJOS_GFAM":        [""] * n,
        "GSE":               [""] * n,
        "REGION_NATURAL":    [""] * n,
        "TipoBase":          [""] * n,
        "Fecha Carga":       [fecha_carga] * n,
        "Orden_Discado":     [99999] * n,
        "cupo":              [""] * n,
        "DESCUENTO":         [""] * n,
        "INSPECCION":        [""] * n,
        "MensajeWSP":        [""] * n,
    })

    # 3b. Separar registros sin teléfono válido (queda vacío cuando el
    # Excel trae la columna phone_number vacía o en un formato no
    # reconocido). No se cargan: se dejan en un archivo aparte para
    # revisión manual — mismo criterio que Carrito Abandonado.
    emit("Separando registros sin teléfono")
    mask_sin_telefono = df_carga["Telefono1"].astype(str).str.strip().isin(["", "0"])
    df_no_cargados = df_carga[mask_sin_telefono].reset_index(drop=True)
    df_carga       = df_carga[~mask_sin_telefono].reset_index(drop=True)

    # 4. Exportar: Carga va a compartida y a local; No Cargados solo a compartida
    emit("Generando archivo Excel")
    nombre_carga       = f"CargaMKT{hoy_compacto}.xls"
    nombre_no_cargados = f"NoCargadosMKT{hoy_compacto}.xls"
    tareas = [
        (df_carga,       nombre_carga,       "Contactos", True,  "carga"),
        (df_no_cargados, nombre_no_cargados, "Contactos", False, "no_cargados"),
    ]
    paths = exportar_multi_destino(tareas, output_dirs, claves_local={"carga"})
    path_carga       = paths["carga"]
    path_no_cargados = paths["no_cargados"]

    # 5. Log (sin repetidos/bloqueados/resoluciones: siempre 0 en este
    #    caso; los "no cargados" por falta de teléfono se registran como
    #    bloqueados para que queden visibles en el log de auditoría)
    try:
        registrar_log(
            tipo_caso="MKT",
            total_entrada=total_entrada,
            total_repetidos=0,
            total_bloqueados=len(df_no_cargados),
            total_carga=len(df_carga),
            archivo_origen=nombre_archivo,
            usuario=usuario,
        )
    except Exception as _e:
        print(f"[WARN] No se pudo registrar log de MKT: {_e}")

    # 6. Marcar en snapshot del watcher (para que el panel muestre
    #    "✓ Procesado" también cuando se procesa manualmente, no solo
    #    por el watcher)
    try:
        from app.core.ftp_watcher import _marcar_procesado, _clave_procesado, _extraer_horario
        horario = _extraer_horario(nombre_archivo or "")
        _marcar_procesado(_clave_procesado("MKT", horario, nombre_archivo or ""))
    except Exception:
        pass

    return {
        "archivo_carga":        path_carga,
        "archivo_no_cargados":  path_no_cargados,
        "total_entrada":        total_entrada,
        "total_carga":          len(df_carga),
        "total_no_cargados":    len(df_no_cargados),
        "total_repetidos":      0,
        "total_bloqueados":     len(df_no_cargados),
        "_archivo_bytes":       archivo_bytes,
        "_nombre_archivo":      nombre_archivo,
    }