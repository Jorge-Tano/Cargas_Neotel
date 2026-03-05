import io
import pandas as pd
from datetime import date
from app.services.utils import (
    agregar_cero,
    concatenar_nombre,
    separar_repetidos,
    exportar_excel,
    leer_archivo,
    nombre_sin_colision,

)
from app.core.sqlserver import get_repetidos
from app.core.postgres import registrar_log
from app.core.ftp import descargar_archivo_sftp


def _col(df, col, default=""):
    if col in df.columns:
        return df[col].fillna("").tolist()
    # Fallback: si piden col con _x y no existe, buscar sin sufijo
    if col.endswith("_x"):
        base = col[:-2]
        if base in df.columns:
            return df[base].fillna("").tolist()
    return [default] * len(df)


def procesar_refi_pl(
    tipo: str,
    output_dir: str = "/tmp",
    archivo_bytes: bytes = None,
    nombre_archivo: str = None,
    progress_cb=None,
    usuario: str = "",
) -> dict:
    """
    Procesa REFI o PL Leakage.
    Si no se pasa archivo_bytes, descarga automáticamente el más reciente del SFTP.
    """
    def emit(step):
        if progress_cb:
            progress_cb(step)

    hoy = date.today().strftime("%Y%m%d")
    fecha_carga = date.today().strftime("%d/%m/%Y")
    tipo = tipo.upper()

    if tipo not in ("REFI", "PL"):
        raise ValueError("tipo debe ser 'REFI' o 'PL'")

    # 1. Obtener archivo desde SFTP o el subido manualmente
    if archivo_bytes is None:
        emit("Descargando desde SFTP")
        archivo_bytes, nombre_archivo = descargar_archivo_sftp(tipo)

    # 2. Leer archivo
    emit("Leyendo archivo")
    df = leer_archivo(archivo_bytes, nombre_archivo)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)

    # 3. Separar repetidos
    emit("Verificando repetidos en base de datos")
    caso_bd = "REFI" if tipo == "REFI" else "PL"
    ruts_repetidos = get_repetidos(caso_bd)
    df_nuevos, df_repetidos = separar_repetidos(df, "RUT", ruts_repetidos)
    df_nuevos = df_nuevos.reset_index(drop=True)
    df_repetidos = df_repetidos.reset_index(drop=True)

    # 4. Sin blacklist para REFI/PL
    df_bloqueados = pd.DataFrame()

    # 5. Construir archivo de carga
    if tipo == "REFI":
        df_carga = _construir_carga_refi(df_nuevos, fecha_carga)
        nombre_carga     = f"CargaRefiLeakage{hoy}.xls"
        nombre_repetidos = f"RegistrosRepetidosREFILeakage{hoy}.xls"
        nombre_bloqueo   = f"BloqueoREFILeakage{hoy}.xls"
    else:
        df_carga = _construir_carga_pl(df_nuevos, fecha_carga)
        nombre_carga     = f"CargaPagoLivianoLeakage{hoy}.xls"
        nombre_repetidos = f"RegistrosRepetidosPLLeakage{hoy}.xls"
        nombre_bloqueo   = f"BloqueoPLLeakage{hoy}.xls"

    # 6. Bloqueo: solo RUT de los que VAN a carga
    df_bloqueo = pd.DataFrame({"RUT": _col(df_nuevos, "RUT")})

    # 7. Exportar en paralelo
    emit("Generando archivos Excel")
    path_carga     = nombre_sin_colision(f"{output_dir}/{nombre_carga}")
    path_repetidos = nombre_sin_colision(f"{output_dir}/{nombre_repetidos}")
    path_bloqueo   = nombre_sin_colision(f"{output_dir}/{nombre_bloqueo}")

    from concurrent.futures import ThreadPoolExecutor as _TPE
    tareas = [
        (df_carga,     path_carga,     "Contactos"),
        (df_repetidos, path_repetidos, "Contactos"),
        (df_bloqueo,   path_bloqueo,   "ESTADO"),
    ]
    with _TPE(max_workers=3) as pool:
        pool.map(lambda t: exportar_excel(t[0], t[1], sheet_name=t[2]), tareas)

    # 8. Log
    registrar_log(
        tipo_caso=tipo,
        total_entrada=total_entrada,
        total_repetidos=len(df_repetidos),
        total_bloqueados=len(df_bloqueados),
        total_carga=len(df_carga),
        archivo_origen=nombre_archivo,
        usuario=usuario,
    )

    return {
        "archivo_carga":     path_carga,
        "archivo_repetidos": path_repetidos,
        "archivo_bloqueo":   path_bloqueo,
        "total_entrada":     total_entrada,
        "total_repetidos":   len(df_repetidos),
        "total_carga":       len(df_carga),
        "_archivo_bytes":    archivo_bytes,
        "_nombre_archivo":   nombre_archivo,
    }


def _get_telefonos(df, prefijos):
    """Retorna lista de telefonos con cero, probando distintos nombres de columna."""
    for col in prefijos:
        if col in df.columns:
            return [agregar_cero(v) for v in _col(df, col)]
    return ["00"] * len(df)


def _construir_carga_refi(df: pd.DataFrame, fecha_carga: str) -> pd.DataFrame:
    n = len(df)
    cols = [
        "Rut","Digito","Nombre","Apellido_Paterno","Apellido_Materno",
        "Fecha_de_Nacimiento","Direccion_Particular","Comuna_Particular","Ciudad_Particular",
        "PAG009","OBM011","OBM015","Sucursal_Deudor",
        "Telefono 1","Teléfono 2","Teléfono 3","Teléfono 4","Teléfono 5","Teléfono 6",
        "Telefono Particular","Telefono 8","Telefono 9","Telefono 10","Telefono 11",
        "PORCENTAJE DEUDA","TipoPropension","MARCA_PROPENSION","PIE","Rango","Clasificación",
        "Orden Discado","TipoBase","FECHAVCTO","PRODUCTO","OBM019","OBM020","Tasa",
        "OBM012","OBM013","Fecha Carga","Identif_Deudor","Codigo_Sexo","Estado_Civil",
        "Empresa_del_deudor","Cargo_del_deudor","Profesion_del_deudor","Pais_Particular",
        "Codigo_Comuna_Part","Direccion_Comercial","Comuna_Comercial","Ciudad_Comercial",
        "Pais_Comercial","PAG007","OBM004","PAG014","PAG017","DCTO_TASA"
    ]
    if n == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "Rut":                  _col(df, "RUT"),
        "Digito":               _col(df, "DV"),
        "Nombre":               _col(df, "NOMBRE_x"),
        "Apellido_Paterno":     _col(df, "APELLIDO_PATERNO_x"),
        "Apellido_Materno":     _col(df, "APELLIDO_MATERNO_x"),
        "Fecha_de_Nacimiento":  [""] * n,
        "Direccion_Particular": _col(df, "DIRECCION_CUENTA_x"),
        "Comuna_Particular":    _col(df, "COMUNA_CUENTA_x"),
        "Ciudad_Particular":    _col(df, "CIUDAD_CUENTA_x"),
        "PAG009":               [""] * n,
        "OBM011":               [""] * n,
        "OBM015":               [""] * n,
        "Sucursal_Deudor":      [""] * n,
        "Telefono 1":           _get_telefonos(df, ["TELEFONO1", "TELEFONO_1"]),
        "Teléfono 2":           _get_telefonos(df, ["TELEFONO2", "TELEFONO_2"]),
        "Teléfono 3":           _get_telefonos(df, ["TELEFONO3", "TELEFONO_3"]),
        "Teléfono 4":           [""] * n,
        "Teléfono 5":           [""] * n,
        "Teléfono 6":           [""] * n,
        "Telefono Particular":  [""] * n,
        "Telefono 8":           [""] * n,
        "Telefono 9":           [""] * n,
        "Telefono 10":          [""] * n,
        "Telefono 11":          [""] * n,
        "PORCENTAJE DEUDA":     [""] * n,
        "TipoPropension":       [""] * n,
        "MARCA_PROPENSION":     [""] * n,
        "PIE":                  [""] * n,
        "Rango":                [""] * n,
        "Clasificación":        [""] * n,
        "Orden Discado":        [99999] * n,
        "TipoBase":             ["RN Leakage"] * n,
        "FECHAVCTO":            [""] * n,
        "PRODUCTO":             _col(df, "PRODUCTO_APP"),
        "OBM019":               [""] * n,
        "OBM020":               [""] * n,
        "Tasa":                 [""] * n,
        "OBM012":               [""] * n,
        "OBM013":               [""] * n,
        "Fecha Carga":          [fecha_carga] * n,
        "Identif_Deudor":       [""] * n,
        "Codigo_Sexo":          [""] * n,
        "Estado_Civil":         [""] * n,
        "Empresa_del_deudor":   [""] * n,
        "Cargo_del_deudor":     [""] * n,
        "Profesion_del_deudor": [""] * n,
        "Pais_Particular":      [""] * n,
        "Codigo_Comuna_Part":   [""] * n,
        "Direccion_Comercial":  [""] * n,
        "Comuna_Comercial":     [""] * n,
        "Ciudad_Comercial":     [""] * n,
        "Pais_Comercial":       [""] * n,
        "PAG007":               [""] * n,
        "OBM004":               [""] * n,
        "PAG014":               [""] * n,
        "PAG017":               [""] * n,
        "DCTO_TASA":            [""] * n
    })


def _construir_carga_pl(df: pd.DataFrame, fecha_carga: str) -> pd.DataFrame:
    n = len(df)
    cols = [
        "Rut","Digito","Nombre","Apellido_Paterno","Apellido_Materno",
        "Fecha_de_Nacimiento","Direccion_Particular","Comuna_Particular","Ciudad_Particular",
        "PAG009","OBM011","OBM015","Sucursal_Deudor",
        "Telefono 1","Teléfono 2","Teléfono 3","Teléfono 4","Teléfono 5","Teléfono 6",
        "Telefono7","Telefono 8","Telefono 9","Telefono 10","Telefono 11",
        "PORCENTAJE DEUDA","TipoPropension","MARCA_PROPENSION","PIE","Rango","Clasificación",
        "Orden Discado","TipoBase","FECHAVCTO","PRODUCTO","OBM019","OBM020",
        "OBM012","OBM013","Tasa","BDD","FechaCarga","Descuento Tasa",
        "MarcaEstrategia","Propension","AV","SAV","Novedad"
    ]
    if n == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "Rut":                  _col(df, "RUT"),
        "Digito":               _col(df, "DV"),
        "Nombre":               _col(df, "NOMBRE"),
        "Apellido_Paterno":     _col(df, "APELLIDO_PATERNO"),
        "Apellido_Materno":     _col(df, "APELLIDO_MATERNO"),
        "Fecha_de_Nacimiento":  [""] * n,
        "Direccion_Particular": _col(df, "DIRECCION_CUENTA_x"),
        "Comuna_Particular":    _col(df, "COMUNA_CUENTA_x"),
        "Ciudad_Particular":    _col(df, "CIUDAD_CUENTA_x"),
        "PAG009":               [""] * n,
        "OBM011":               [""] * n,
        "OBM015":               [""] * n,
        "Sucursal_Deudor":      [""] * n,
        "Telefono 1":           _get_telefonos(df, ["TELEFONO1", "TELEFONO_1"]),
        "Teléfono 2":           _get_telefonos(df, ["TELEFONO2", "TELEFONO_2"]),
        "Teléfono 3":           _get_telefonos(df, ["TELEFONO3", "TELEFONO_3"]),
        "Teléfono 4":           [""] * n,
        "Teléfono 5":           [""] * n,
        "Teléfono 6":           [""] * n,
        "Telefono7":            [""] * n,
        "Telefono 8":           [""] * n,
        "Telefono 9":           [""] * n,
        "Telefono 10":          [""] * n,
        "Telefono 11":          [""] * n,
        "PORCENTAJE DEUDA":     [""] * n,
        "TipoPropension":       [""] * n,
        "MARCA_PROPENSION":     [""] * n,
        "PIE":                  [""] * n,
        "Rango":                [""] * n,
        "Clasificación":        [""] * n,
        "Orden Discado":        [99999] * n,
        "TipoBase":             ["PL Leakage"] * n,
        "FECHAVCTO":            [""] * n,
        "PRODUCTO":             _col(df, "PRODUCTO_APP"),
        "OBM019":               [""] * n,
        "OBM020":               [""] * n,
        "OBM012":               [""] * n,
        "OBM013":               [""] * n,
        "Tasa":                 [""] * n,
        "BDD":                  [""] * n,
        "FechaCarga":           [fecha_carga] * n,
        "Descuento Tasa":       [""] * n,
        "MarcaEstrategia":      [""] * n,
        "Propension":           [""] * n,
        "AV":                   [""] * n,
        "SAV":                  [""] * n,
        "Novedad":              [""] * n,
    })