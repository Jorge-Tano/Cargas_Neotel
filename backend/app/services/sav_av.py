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
    nombre_sin_colision
)
from app.core.sqlserver import get_repetidos, get_contactos_efectivos_5757
from app.core.postgres import get_lista_negra, registrar_log, registrar_repetidos


def _col(df, col, default=""):
    """Retorna lista de valores de columna o lista de defaults."""
    if col in df.columns:
        return df[col].fillna("").tolist()
    return [default] * len(df)


def _normalizar_columnas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    """
    Normaliza columnas al formato estándar.
    Siempre aplica el mapeo de oferta/monto, sin importar el formato del archivo.

    Formato normal SAV:  RUT, DV, NOMBRE, APELLIDO_PATERNO, APELLIDO_MATERNO,
                         TELEFONO_1, TELEFONO_2, TELEFONO_3, OFERTA_MAXIMA,
                         SEGURO_DESGRAVAMEN, SEGURO_INTEGRAL
    Formato alt SAV:     Rut, DV, Nombre, Paterno, Materno,
                         Telefono, TELEFONO 2..10, M OFERTA BDD,
                         SEGURO_DESGRAVAMEN, SEGURO_INTEGRAL

    Formato normal AV:   RUT, DV, NOMBRE, APELLIDO_PATERNO, APELLIDO_MATERNO,
                         TELEFONO_1, TELEFONO_2, TELEFONO_3, MONTO_AVANCE
    Formato alt AV:      Rut, DV, Nombre, Paterno, Materno,
                         Telefono, TELEFONO 2..10, M OFERTA
    """
    df = df.copy()
    df.columns = df.columns.str.strip()

    renames = {}

    # --- Identidad (solo si vienen en formato alternativo) ---
    if "Rut"     in df.columns: renames["Rut"]     = "RUT"
    if "Nombre"  in df.columns: renames["Nombre"]  = "NOMBRE"
    if "Paterno" in df.columns: renames["Paterno"] = "APELLIDO_PATERNO"
    if "Materno" in df.columns: renames["Materno"] = "APELLIDO_MATERNO"

    # --- Teléfonos (solo si vienen en formato alternativo) ---
    if "Telefono"   in df.columns: renames["Telefono"]   = "TELEFONO_1"
    if "TELEFONO 2" in df.columns: renames["TELEFONO 2"] = "TELEFONO_2"
    if "TELEFONO 3" in df.columns: renames["TELEFONO 3"] = "TELEFONO_3"

    # --- Oferta: SIEMPRE buscar sin importar el formato ---
    # Posibles nombres de la columna fuente según tipo:
    #   SAV: OFERTA_MAXIMA, M OFERTA BDD, M OFERTA
    #   AV:  MONTO_AVANCE, OFERTA_MAXIMA, M OFERTA BDD, M OFERTA
    col_destino_oferta = "OFERTA_MAXIMA" if tipo == "SAV" else "MONTO_AVANCE"

    if col_destino_oferta not in df.columns:
        if tipo == "SAV":
            # Prioridad: OFERTA_MAXIMA > M OFERTA BDD > M OFERTA
            col_src = next(
                (c for c in df.columns if c.strip().upper() == "OFERTA_MAXIMA"), None
            ) or next(
                (c for c in df.columns if c.strip().upper() == "M OFERTA BDD"), None
            ) or next(
                (c for c in df.columns if "M OFERTA" in c.upper()), None
            )
        else:
            # AV: el archivo puede traer MONTO_AVANCE, OFERTA_MAXIMA, M OFERTA BDD o M OFERTA
            col_src = next(
                (c for c in df.columns if c.strip().upper() == "MONTO_AVANCE"), None
            ) or next(
                (c for c in df.columns if c.strip().upper() == "OFERTA_MAXIMA"), None
            ) or next(
                (c for c in df.columns if c.strip().upper() == "M OFERTA BDD"), None
            ) or next(
                (c for c in df.columns if "M OFERTA" in c.upper()), None
            )
        if col_src:
            renames[col_src] = col_destino_oferta

    df = df.rename(columns=renames)

    # --- Limpiar montos: quitar puntos, comas y espacios ---
    # Ej: " 1.010.637 " → "1010637"
    for col in ["OFERTA_MAXIMA", "MONTO_AVANCE"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.strip()
                .str.replace(".", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
            )

    return df


def procesar_sav_av(
    archivo_bytes: bytes = None,
    nombre_archivo: str = None,
    tipo: str = "SAV",
    output_dir: str = "/tmp",
    progress_cb=None,
    usuario: str = "",
) -> dict:
    def emit(step):
        if progress_cb:
            progress_cb(step)

    hoy = date.today().strftime("%Y%m%d")
    dia = str(int(date.today().strftime("%d")))
    fecha_carga = date.today().strftime("%d/%m/%Y")
    tipo = tipo.upper()

    if tipo not in ("SAV", "AV"):
        raise ValueError("tipo debe ser 'SAV' o 'AV'")

    # 1. Obtener archivo desde SFTP o el subido manualmente
    if archivo_bytes is None:
        emit("Descargando desde SFTP")
        from app.core.ftp import descargar_archivo_sftp
        archivo_bytes, nombre_archivo = descargar_archivo_sftp(tipo)
        emit(f"Archivo: {nombre_archivo}")

    # 2. Leer archivo
    emit("Leyendo archivo")
    df = leer_archivo(archivo_bytes, nombre_archivo)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)
    df_original = df.copy()
    df = _normalizar_columnas(df, tipo)

    # 2. Verificar repetidos
    caso_bd = "SAV_AV" if tipo == "SAV" else "AV"
    emit("Verificando repetidos")
    ruts_repetidos = get_repetidos(caso_bd)
    col_rut = "RUT" if "RUT" in df.columns else "Rut"
    ruts_archivo = df[col_rut].astype(str).str.strip().tolist()

    idx_repetidos = [i for i, r in enumerate(ruts_archivo) if r in ruts_repetidos]
    idx_nuevos    = [i for i, r in enumerate(ruts_archivo) if r not in ruts_repetidos]
    df_repetidos = df_original.iloc[idx_repetidos].reset_index(drop=True)
    df_nuevos    = df.iloc[idx_nuevos].reset_index(drop=True)

    # 3. Cruzar lista negra
    emit("Cruzando lista negra")
    lista_negra = get_lista_negra()
    if "TELEFONO_1" in df_nuevos.columns:
        df_nuevos, df_bloqueados = separar_lista_negra(df_nuevos, "TELEFONO_1", lista_negra)
        df_nuevos = df_nuevos.reset_index(drop=True)
        df_bloqueados = df_bloqueados.reset_index(drop=True)
    else:
        df_bloqueados = pd.DataFrame()

    # 4. Formatear telefonos
    emit("Formateando teléfonos")
    cols_tel = [c for c in df_nuevos.columns if "TELEFONO" in c.upper()]
    df_nuevos = formatear_columnas_telefono(df_nuevos, cols_tel)
    df_nuevos = df_nuevos.reset_index(drop=True)

    # 5. Filtrar registros con monto menor a $700.000
    emit("Filtrando ofertas menores a $700.000")
    MONTO_MINIMO = 700_000
    col_monto = "OFERTA_MAXIMA" if tipo == "SAV" else "MONTO_AVANCE"

    if col_monto in df_nuevos.columns:
        montos = pd.to_numeric(df_nuevos[col_monto], errors="coerce")
        mask_validos = montos.isna() | (montos >= MONTO_MINIMO)
        df_descartados_monto = df_nuevos[~mask_validos].reset_index(drop=True)
        df_nuevos            = df_nuevos[mask_validos].reset_index(drop=True)
    else:
        df_descartados_monto = pd.DataFrame()

    # 6. Construir archivos de salida
    if tipo == "SAV":
        df_carga = _construir_carga_sav(df_nuevos, fecha_carga, dia)
        nombre_carga             = f"CargaSavLeakage{hoy}.xls"
        nombre_repetidos         = f"RegistrosRepetidosSAVLeakage{hoy}.xls"
        nombre_bloqueo           = f"BloqueoSAVLeakage{hoy}.xls"
        nombre_blacklist         = f"BlackListSAVLeakage{hoy}.xls"
        nombre_descartados_monto = f"DescartadosMontoSAVLeakage{hoy}.xls"
    else:
        df_carga = _construir_carga_av(df_nuevos, fecha_carga, dia)
        nombre_carga             = f"CargaLeakageAv{hoy}.xls"
        nombre_repetidos         = f"RegistrosRepetidosAVLeakage{hoy}.xls"
        nombre_bloqueo           = f"BloqueoAVLeakage{hoy}.xls"
        nombre_blacklist         = f"BlackListAVLeakage{hoy}.xls"
        nombre_descartados_monto = f"DescartadosMontoAVLeakage{hoy}.xls"

    df_bloqueo = pd.DataFrame({"RUT": _col(df_nuevos, "RUT")})

    # 7. Exportar en paralelo
    emit("Generando archivos Excel")
    path_carga             = nombre_sin_colision(f"{output_dir}/{nombre_carga}")
    path_repetidos         = nombre_sin_colision(f"{output_dir}/{nombre_repetidos}")
    path_bloqueo           = nombre_sin_colision(f"{output_dir}/{nombre_bloqueo}")
    path_blacklist         = nombre_sin_colision(f"{output_dir}/{nombre_blacklist}")
    path_descartados_monto = nombre_sin_colision(f"{output_dir}/{nombre_descartados_monto}")

    from concurrent.futures import ThreadPoolExecutor as _TPE
    tareas = [
        (df_carga,             path_carga,             "Contactos", True),
        (df_repetidos,         path_repetidos,         "Contactos", False),
        (df_bloqueo,           path_bloqueo,           "ESTADO",    True),
        (df_bloqueados,        path_blacklist,         "ESTADO",    True),
        (df_descartados_monto, path_descartados_monto, "Contactos", False),
    ]
    with _TPE(max_workers=4) as pool:
        pool.map(lambda t: exportar_excel(t[0], t[1], sheet_name=t[2], reprocesar=t[3]), tareas)
    # 9. Log
    registrar_log(
        tipo_caso=tipo,
        total_entrada=total_entrada,
        total_repetidos=len(df_repetidos),
        total_bloqueados=len(df_bloqueados),
        total_carga=len(df_carga),
        archivo_origen=nombre_archivo,
        usuario=usuario,
    )
    if len(df_repetidos) > 0 and col_rut in df_repetidos.columns:
        registrar_repetidos(
            ruts=df_repetidos[col_rut].astype(str).str.strip().tolist(),
            tipo_caso=tipo,
        )

    return {
        "archivo_carga":             path_carga,
        "archivo_repetidos":         path_repetidos,
        "archivo_bloqueo":           path_bloqueo,
        "archivo_blacklist":         path_blacklist,
        "archivo_descartados_monto": path_descartados_monto,
        "total_entrada":             total_entrada,
        "total_repetidos":           len(df_repetidos),
        "total_bloqueados":          len(df_bloqueados),
        "total_descartados_monto":   len(df_descartados_monto),
        "total_carga":               len(df_carga),
        "_archivo_bytes":            archivo_bytes,
        "_nombre_archivo":           nombre_archivo,
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