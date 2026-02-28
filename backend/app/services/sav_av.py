import pandas as pd
from datetime import date
from app.services.utils import (
    agregar_cero,
    formatear_columnas_telefono,
    concatenar_nombre,
    separar_repetidos,
    separar_lista_negra,
    aplicar_contacto_efectivo,
    exportar_excel,
    leer_archivo,
)
from app.core.sqlserver import get_repetidos, get_contactos_efectivos_5757
from app.core.postgres import get_lista_negra, registrar_log


def _col(df, col, default=""):
    """Retorna lista de valores de columna o lista de defaults."""
    if col in df.columns:
        return df[col].fillna("").tolist()
    return [default] * len(df)


def procesar_sav_av(
    archivo_bytes: bytes,
    nombre_archivo: str,
    tipo: str,
    output_dir: str = "/tmp"
) -> dict:
    hoy = date.today().strftime("%Y%m%d")
    dia = str(int(date.today().strftime("%d")))  # Sin cero adelante: "28" no "028"
    fecha_carga = date.today().strftime("%d/%m/%Y")
    tipo = tipo.upper()

    if tipo not in ("SAV", "AV"):
        raise ValueError("tipo debe ser 'SAV' o 'AV'")

    # 1. Leer archivo
    df = leer_archivo(archivo_bytes, nombre_archivo)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)

    # 2. Separar repetidos (con toda la informacion del registro)
    caso_bd = "SAV_AV" if tipo == "SAV" else "AV"
    ruts_repetidos = get_repetidos(caso_bd)
    df_nuevos, df_repetidos = separar_repetidos(df, "RUT", ruts_repetidos)
    df_nuevos = df_nuevos.reset_index(drop=True)
    df_repetidos = df_repetidos.reset_index(drop=True)

    # 3. Contactos efectivos 5757
    contactos_efectivos = get_contactos_efectivos_5757()
    if "TELEFONO_1" in df_nuevos.columns:
        df_nuevos = aplicar_contacto_efectivo(df_nuevos, "RUT", "TELEFONO_1", contactos_efectivos)
        df_nuevos = df_nuevos.reset_index(drop=True)

    # 4. Cruzar lista negra
    lista_negra = get_lista_negra()
    if "TELEFONO_1" in df_nuevos.columns:
        df_nuevos, df_bloqueados = separar_lista_negra(df_nuevos, "TELEFONO_1", lista_negra)
        df_nuevos = df_nuevos.reset_index(drop=True)
    else:
        df_bloqueados = pd.DataFrame()

    # 5. Formatear telefonos
    cols_tel = [c for c in df_nuevos.columns if "TELEFONO" in c.upper()]
    df_nuevos = formatear_columnas_telefono(df_nuevos, cols_tel)
    df_nuevos = df_nuevos.reset_index(drop=True)

    # 6. Construir archivos de salida
    if tipo == "SAV":
        df_carga = _construir_carga_sav(df_nuevos, fecha_carga, dia)
        nombre_carga     = f"CargaSavLeakage{hoy}.xls"
        nombre_repetidos = f"RegistrosRepetidosSAVLeakage{hoy}.xls"
        nombre_bloqueo   = f"BloqueoSAVLeakage{hoy}.xls"
    else:
        df_carga = _construir_carga_av(df_nuevos, fecha_carga, dia)
        nombre_carga     = f"CargaLeakageAv{hoy}.xls"
        nombre_repetidos = f"RegistrosRepetidosAVLeakage{hoy}.xls"
        nombre_bloqueo   = f"BloqueoAVLeakage{hoy}.xls"

    # 7. Bloqueo: solo RUT de los que VAN a carga
    df_bloqueo = pd.DataFrame({"RUT": _col(df_nuevos, "RUT")})

    # 7b. BlackList: info completa de los bloqueados por lista negra
    if tipo == "SAV":
        nombre_blacklist = f"BlackListSAVLeakage{hoy}.xls"
    else:
        nombre_blacklist = f"BlackListAVLeakage{hoy}.xls"

    # 8. Exportar
    path_carga      = f"{output_dir}/{nombre_carga}"
    path_repetidos  = f"{output_dir}/{nombre_repetidos}"
    path_bloqueo    = f"{output_dir}/{nombre_bloqueo}"
    path_blacklist  = f"{output_dir}/{nombre_blacklist}"

    exportar_excel(df_carga,      path_carga)
    exportar_excel(df_repetidos,  path_repetidos)
    exportar_excel(df_bloqueo,    path_bloqueo)
    exportar_excel(df_bloqueados, path_blacklist)

    # 9. Log
    registrar_log(
        tipo_caso=tipo,
        total_entrada=total_entrada,
        total_repetidos=len(df_repetidos),
        total_bloqueados=len(df_bloqueados),
        total_carga=len(df_carga),
        archivo_origen=nombre_archivo,
    )

    return {
        "archivo_carga":      path_carga,
        "archivo_repetidos":  path_repetidos,
        "archivo_bloqueo":    path_bloqueo,
        "archivo_blacklist":  path_blacklist,
        "total_entrada":      total_entrada,
        "total_repetidos":    len(df_repetidos),
        "total_bloqueados":   len(df_bloqueados),
        "total_carga":        len(df_carga),
    }


def _construir_carga_sav(df: pd.DataFrame, fecha_carga: str, dia: str) -> pd.DataFrame:
    """
    Construye el DataFrame de carga SAV.
    Columnas requeridas segun plantilla.
    """
    n = len(df)
    if n == 0:
        # Retornar DataFrame vacio con las columnas correctas
        cols = [
            "FechaSimulacion","Categoria","CELULAR","EMAIL","TRANSACCION","CANAL",
            "COD_CANAL","SUBCOD_CANAL","SITIO","PRODUCTO","COD_PRODUCTO","MONTO_OFERTA",
            "MONTO_SIMULADO","PLAZO","TASA_MENSUAL","ETAPA","SEGURO_DESGRAVAMEN",
            "SEGURO_INTEGRAL","RUT","DV","NOMBRE","SEXO","FECHA NACIMIENTO","EDAD",
            "PRODUCTO_TARJETA","SERIE","DIRECCION","COMUNA","OFERTA MAXIMA","PLAZO MAXIMO",
            "CLAVE DINAMICA","TipoBase","TELEFONO","TELEFONO 2","TELEFONO 3","TELEFONO 4",
            "TELEFONO 5","TELEFONO 6","TELEFONO 7","TELEFONO 8","TELEFONO 9",
            "MARCA_SEGMENTO","Campana","Propensos","RESPECTIVO","CUOTAS","FECHA CARGA",
            "DetalleOferta","Orden Discado","Prioridad","Cantidad RUT","FechaMinima",
            "FechaMaxima","NUEVA_OFERTA","Accion","OfertaMes","descuento","Antiguedad"
        ]
        return pd.DataFrame(columns=cols)

    nombres = [
        concatenar_nombre(df.iloc[i], "NOMBRE", "APELLIDO_PATERNO", "APELLIDO_MATERNO")
        for i in range(n)
    ]

    return pd.DataFrame({
        "FechaSimulacion":    ["N/A"] * n,
        "Categoria":          [""] * n,
        "CELULAR":            [""] * n,
        "EMAIL":              [""] * n,
        "TRANSACCION":        [""] * n,
        "CANAL":              [""] * n,
        "COD_CANAL":          [""] * n,
        "SUBCOD_CANAL":       [""] * n,
        "SITIO":              [""] * n,
        "PRODUCTO":           [""] * n,
        "COD_PRODUCTO":       [""] * n,
        "MONTO_OFERTA":       _col(df, "OFERTA_MAXIMA"),
        "MONTO_SIMULADO":     [""] * n,
        "PLAZO":              [""] * n,
        "TASA_MENSUAL":       [""] * n,
        "ETAPA":              [""] * n,
        "SEGURO_DESGRAVAMEN": _col(df, "SEGURO_DESGRAVAMEN"),
        "SEGURO_INTEGRAL":    _col(df, "SEGURO_INTEGRAL"),
        "RUT":                _col(df, "RUT"),
        "DV":                 _col(df, "DV"),
        "NOMBRE":             nombres,
        "SEXO":               _col(df, "SEXO"),
        "FECHA NACIMIENTO":   [""] * n,
        "EDAD":               _col(df, "EDAD"),
        "PRODUCTO_TARJETA":   [""] * n,
        "SERIE":              _col(df, "SERIE"),
        "DIRECCION":          [""] * n,
        "COMUNA":             [""] * n,
        "OFERTA MAXIMA":      _col(df, "OFERTA_MAXIMA"),
        "PLAZO MAXIMO":       [""] * n,
        "CLAVE DINAMICA":     [""] * n,
        "TipoBase":           ["NORMAL"] * n,
        "TELEFONO":           [agregar_cero(v) for v in _col(df, "TELEFONO_1")],
        "TELEFONO 2":         [agregar_cero(v) for v in _col(df, "TELEFONO_2")],
        "TELEFONO 3":         [agregar_cero(v) for v in _col(df, "TELEFONO_3")],
        "TELEFONO 4":         ["00"] * n,
        "TELEFONO 5":         ["00"] * n,
        "TELEFONO 6":         ["00"] * n,
        "TELEFONO 7":         ["00"] * n,
        "TELEFONO 8":         ["00"] * n,
        "TELEFONO 9":         ["00"] * n,
        "MARCA_SEGMENTO":     [""] * n,
        "Campana":            [""] * n,
        "Propensos":          [""] * n,
        "RESPECTIVO":         [""] * n,
        "CUOTAS":             [""] * n,
        "FECHA CARGA":        [fecha_carga] * n,
        "DetalleOferta":      [""] * n,
        "Orden Discado":      [dia] * n,
        "Prioridad":          [""] * n,
        "Cantidad RUT":       [""] * n,
        "FechaMinima":        [""] * n,
        "FechaMaxima":        [""] * n,
        "NUEVA_OFERTA":       [""] * n,
        "Accion":             [""] * n,
        "OfertaMes":          [""] * n,
        "descuento":          [""] * n,
        "Antiguedad":         [""] * n,
    })


def _construir_carga_av(df: pd.DataFrame, fecha_carga: str, dia: str) -> pd.DataFrame:
    """Construye el DataFrame de carga AV."""
    n = len(df)
    if n == 0:
        return pd.DataFrame(columns=[
            "FechaSimulacion","Categoria","CELULAR","EMAIL","TRANSACCION","CANAL",
            "COD_CANAL","SUBCOD_CANAL","SITIO","PRODUCTO","COD_PRODUCTO","MONTO_OFERTA",
            "MONTO_SIMULADO","PLAZO","TASA_MENSUAL","ETAPA","SEGURO_DESGRAVAMEN",
            "SEGURO_INTEGRAL","Rut","Digito","Nombre Cliente","SEXO","FECHA_NACI","EDAD",
            "PRODUCTO_TARJETA","SERIE","Dirección","Comuna","OfertaMaxima","PlazoMaximo",
            "CLAVE_DINAMICA","TipoBase","Telefono1","Telefono2","Telefono3","Telefono4",
            "Telefono5","Telefono6","Telefono7","Telefono8","Telefono9","MARCA_SEGMENTO",
            "CAMPANA","PROPENSOS","RESPECTIVO","FechaCarga","DetalleOferta","OrdenDiscado",
            "AumentoCupo","NVO_CUPO","Descuento","MarcaBot"
        ])

    nombres = [
        concatenar_nombre(df.iloc[i], "NOMBRE", "APELLIDO_PATERNO", "APELLIDO_MATERNO")
        for i in range(n)
    ]

    return pd.DataFrame({
        "FechaSimulacion":    ["N/A"] * n,
        "Categoria":          [""] * n,
        "CELULAR":            [""] * n,
        "EMAIL":              [""] * n,
        "TRANSACCION":        [""] * n,
        "CANAL":              [""] * n,
        "COD_CANAL":          [""] * n,
        "SUBCOD_CANAL":       [""] * n,
        "SITIO":              [""] * n,
        "PRODUCTO":           [""] * n,
        "COD_PRODUCTO":       [""] * n,
        "MONTO_OFERTA":       _col(df, "MONTO_AVANCE"),
        "MONTO_SIMULADO":     [""] * n,
        "PLAZO":              [""] * n,
        "TASA_MENSUAL":       [""] * n,
        "ETAPA":              [""] * n,
        "SEGURO_DESGRAVAMEN": [""] * n,
        "SEGURO_INTEGRAL":    [""] * n,
        "Rut":                _col(df, "RUT"),
        "Digito":             _col(df, "DV"),
        "Nombre Cliente":     nombres,
        "SEXO":               _col(df, "SEXO"),
        "FECHA_NACI":         [""] * n,
        "EDAD":               _col(df, "EDAD"),
        "PRODUCTO_TARJETA":   _col(df, "TIPO_PRODUCTO"),
        "SERIE":              _col(df, "SERIE"),
        "Dirección":          _col(df, "DIRECCION_CUENTA"),
        "Comuna":             _col(df, "COMUNA_CUENTA"),
        "OfertaMaxima":       _col(df, "MONTO_AVANCE"),
        "PlazoMaximo":        [""] * n,
        "CLAVE_DINAMICA":     [""] * n,
        "TipoBase":           ["ACTIVO"] * n,
        "Telefono1":          [agregar_cero(v) for v in _col(df, "TELEFONO_1")],
        "Telefono2":          [agregar_cero(v) for v in _col(df, "TELEFONO_2")],
        "Telefono3":          [agregar_cero(v) for v in _col(df, "TELEFONO_3")],
        "Telefono4":          ["00"] * n,
        "Telefono5":          ["00"] * n,
        "Telefono6":          ["00"] * n,
        "Telefono7":          ["00"] * n,
        "Telefono8":          ["00"] * n,
        "Telefono9":          ["00"] * n,
        "MARCA_SEGMENTO":     [""] * n,
        "CAMPANA":            [""] * n,
        "PROPENSOS":          [""] * n,
        "RESPECTIVO":         [""] * n,
        "FechaCarga":         [fecha_carga] * n,
        "DetalleOferta":      [""] * n,
        "OrdenDiscado":       [dia] * n,
        "AumentoCupo":        [""] * n,
        "NVO_CUPO":           [""] * n,
        "Descuento":          [""] * n,
        "MarcaBot":           [""] * n,
    })