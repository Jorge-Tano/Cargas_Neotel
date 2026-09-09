"""
Caso: Opciones de Pago (2 variantes, mismo layout de salida)
─────────────────────────────────────────────
Dos fuentes de datos distintas que alimentan la MISMA campaña de
negocio, pero van a campañas de Neotel SEPARADAS (rotan mes a mes por
su cuenta, ver ECRM_0290 catálogo "Perdidas Amalia {mes}" / "WHTSP {mes}"):

  - "PERDIDAS" (variante = "perdidas"): entrada solo con teléfono
    (columna "Fono", formato internacional 56XXXXXXXXX). Salida:
    CargaAmaliaOpcionesPago{fecha}.xls — solo Telefono 1 poblado (el
    archivo de referencia real tampoco trae FechaCarga poblada en esta
    variante, se respeta igual).

  - "WHATSAPP" (variante = "whatsapp"): entrada con teléfono + RUT + DV
    + Nombre. Salida: CargaWHTSPOpcionesPago{fecha}.xls — Rut, Digito,
    Nombre, Telefono 1 y FechaCarga (AAAA-MM-DD 00:00:00) poblados.

Ambas comparten el mismo layout de 38 columnas (calcado del builder que
ya existe para REFI — ver refi_pl._construir_carga_refi).

Subida a Neotel: TXT con este mismo layout, sube a /UPLOAD/cargas_manual,
dispara ExecuteTask00 (caso "OP_PERDIDAS" u "OP_WHATSAPP" según
variante) y confirma vía LOTES — solo si el usuario logueado es
PERDIDAS_USUARIO_AUTORIZADO (mismo .env que Llamadas Perdidas/Líder
Amalia). Para cualquier otro usuario, se genera solo el Excel, sin
tocar Neotel (y sin ver la tarjeta en la UI, ver page.tsx CASOS_SOLO_JORGE).
"""

import re
import pandas as pd
from datetime import date, datetime

from app.core.postgres import registrar_log
from app.services.utils import agregar_cero, exportar_multi_destino, exportar_txt_carga, extraer_horario_archivo, leer_archivo

COLUMNAS_SALIDA = [
    "Rut", "Digito", "Nombre", "Apellido_Paterno", "Apellido_Materno",
    "Fecha_de_Nacimiento", "Direccion_Particular", "Comuna_Particular", "Ciudad_Particular",
    "PAG009", "OBM011", "OBM015", "Sucursal_Deudor",
    "Telefono 1", "Teléfono 2", "Teléfono 3", "Teléfono 4", "Teléfono 5", "Teléfono 6",
    "Telefono 8", "Telefono 9", "Telefono 10", "Telefono 11",
    "PORCENTAJE DEUDA", "TipoPropension", "MARCA_PROPENSION", "PIE", "Rango", "Clasificación",
    "Orden Discado", "TipoBase", "FECHAVCTO", "PRODUCTO", "OBM019", "OBM020", "OBM012", "OBM013",
    "Tasa", "FechaCarga",
]

_RE_TEL_INTL = re.compile(r"^\+?56(\d{9})$")

_ARCHIVO_SALIDA = {
    "perdidas":  "CargaAmaliaOpcionesPago",
    "whatsapp":  "CargaWHTSPOpcionesPago",
}
_CASO_CONFIRMACION = {
    "perdidas": "OP_PERDIDAS",
    "whatsapp": "OP_WHATSAPP",
}
_SALIDA_TXT = {
    "perdidas": "SalidaOpcionesPagoPerdidas",
    "whatsapp": "SalidaOpcionesPagoWhatsapp",
}


def _reconstruir_telefono(telefono_raw: str) -> str:
    """"56998296895" -> "0998296895"."""
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


def procesar_opciones_pago(
    variante: str,
    archivo_bytes: bytes,
    nombre_archivo: str,
    output_dirs: dict = None,
    progress_cb=None,
    usuario: str = "",
    procesar_neotel: bool = True,
) -> dict:
    """`variante`: "perdidas" o "whatsapp" — ver docstring del módulo."""
    if variante not in _ARCHIVO_SALIDA:
        raise ValueError(f"variante debe ser 'perdidas' o 'whatsapp', llegó '{variante}'")

    def emit(step):
        if progress_cb:
            progress_cb(step)

    output_dirs = output_dirs or {}
    hoy_compacto = date.today().strftime("%Y%m%d")
    caso = _CASO_CONFIRMACION[variante]

    # 1. Leer archivo
    emit("Leyendo archivo")
    df = leer_archivo(archivo_bytes, nombre_archivo)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)
    n = len(df)

    # 2. Identificar columna de teléfono
    emit("Identificando columna de teléfono")
    col_telefono = None
    for col in df.columns:
        if "fono" in col.lower() or "tel" in col.lower():
            col_telefono = col
            break
    if col_telefono is None:
        raise ValueError("No se encontró columna de teléfono en el archivo de entrada.")
    telefonos = [agregar_cero(_reconstruir_telefono(v)) for v in _col(df, col_telefono)]

    # 3. Identidad (solo variante whatsapp)
    if variante == "whatsapp":
        ruts     = _col(df, "RUT")
        digitos  = _col(df, "DV")
        nombres  = _col(df, "Nombre")
        fecha_carga_valor = [f"{date.today():%Y-%m-%d} 00:00:00"] * n
    else:
        ruts = digitos = nombres = [""] * n
        fecha_carga_valor = [""] * n  # la variante "perdidas" no trae FechaCarga en el archivo real de referencia

    # 4. Construir Carga (38 columnas, resto vacío)
    emit("Construyendo archivo de carga")
    vacio = [""] * n
    df_carga = pd.DataFrame({
        "Rut": ruts, "Digito": digitos, "Nombre": nombres,
        "Apellido_Paterno": vacio, "Apellido_Materno": vacio,
        "Fecha_de_Nacimiento": vacio, "Direccion_Particular": vacio, "Comuna_Particular": vacio, "Ciudad_Particular": vacio,
        "PAG009": vacio, "OBM011": vacio, "OBM015": vacio, "Sucursal_Deudor": vacio,
        "Telefono 1": telefonos, "Teléfono 2": vacio, "Teléfono 3": vacio, "Teléfono 4": vacio, "Teléfono 5": vacio, "Teléfono 6": vacio,
        "Telefono 8": vacio, "Telefono 9": vacio, "Telefono 10": vacio, "Telefono 11": vacio,
        "PORCENTAJE DEUDA": vacio, "TipoPropension": vacio, "MARCA_PROPENSION": vacio, "PIE": vacio, "Rango": vacio, "Clasificación": vacio,
        "Orden Discado": [99999] * n, "TipoBase": vacio, "FECHAVCTO": vacio, "PRODUCTO": vacio, "OBM019": vacio, "OBM020": vacio, "OBM012": vacio, "OBM013": vacio,
        "Tasa": vacio, "FechaCarga": fecha_carga_valor,
    })[COLUMNAS_SALIDA]  # fuerza el orden real

    # 4b. Separar registros sin teléfono válido
    emit("Separando registros sin teléfono")
    mask_sin_telefono = df_carga["Telefono 1"].astype(str).str.strip().isin(["", "00"])
    df_sin_telefono = df_carga[mask_sin_telefono].reset_index(drop=True)
    df_carga        = df_carga[~mask_sin_telefono].reset_index(drop=True)

    # 5. Generar TXT y subir por FTP a Neotel + disparar import — solo si
    #    procesar_neotel viene en True (ya decidido según permisos por
    #    usuario antes de llegar acá, ver main._neotel_efectivo).
    carga_forzada = False
    hora_disparo = None
    path_carga_txt = None
    if len(df_carga) > 0 and procesar_neotel:
        emit("Generando archivo de carga en TXT para Neotel")
        carpeta_txt = output_dirs.get("compartida") or output_dirs.get("local") or "/tmp"
        horario_txt = extraer_horario_archivo(nombre_archivo or "") or datetime.now().strftime("%H%M")
        nombre_carga_txt = f"{_SALIDA_TXT[variante]}{hoy_compacto}{horario_txt}.txt"
        path_carga_txt = exportar_txt_carga(df_carga, f"{carpeta_txt}/{nombre_carga_txt}", COLUMNAS_SALIDA)

        if path_carga_txt:
            emit("Subiendo TXT de carga por FTP a Neotel")
            try:
                from app.core.ftp_neotel17 import subir_archivo_carga_txt
                subir_archivo_carga_txt(path_carga_txt, tipo=caso)
            except Exception as e:
                print(f"[WARN] Error subiendo TXT por FTP: {e}")

            emit("Disparando import inmediato en Neotel")
            try:
                from app.core.sqlserver import obtener_hora_neotel
                from app.core.neotel_ws import ejecutar_tarea
                hora_disparo = obtener_hora_neotel()
                carga_forzada = ejecutar_tarea(caso) is not None
            except Exception as e:
                print(f"[WARN] Error disparando import en Neotel: {e}")

        try:
            from app.core.confirmacion_carga import confirmar_carga_en_segundo_plano
            confirmar_carga_en_segundo_plano(
                caso=caso,
                valores=df_carga["Telefono 1"].astype(str).tolist(),
                columna="TELTELEFONO1",
                archivo_origen=nombre_archivo,
                usuario=usuario,
                carga_forzada=carga_forzada,
                hora_disparo=hora_disparo,
            )
        except Exception as e:
            print(f"[WARN] Error iniciando confirmación de carga {caso} en Neotel: {e}")

    # 6. Exportar Excel
    emit("Generando archivo Excel")
    nombre_carga        = f"{_ARCHIVO_SALIDA[variante]}{hoy_compacto}.xls"
    nombre_sin_telefono = f"SinTelefono{_ARCHIVO_SALIDA[variante]}{hoy_compacto}.xls"
    tareas = [
        (df_carga,        nombre_carga,        "Contactos", True,  "carga"),
        (df_sin_telefono, nombre_sin_telefono, "Contactos", False, "sin_telefono"),
    ]
    paths = exportar_multi_destino(tareas, output_dirs, claves_local={"carga"})
    path_carga        = paths["carga"]
    path_sin_telefono = paths["sin_telefono"]

    # 7. Log
    registrar_log(
        tipo_caso=caso,
        total_entrada=total_entrada,
        total_repetidos=0,
        total_bloqueados=len(df_sin_telefono),
        total_carga=len(df_carga),
        archivo_origen=nombre_archivo,
        usuario=usuario,
    )

    return {
        "archivo_carga":        path_carga,
        "archivo_carga_txt":    path_carga_txt,
        "archivo_sin_telefono": path_sin_telefono,
        "total_entrada":        total_entrada,
        "total_carga":          len(df_carga),
        "total_sin_telefono":   len(df_sin_telefono),
        "_archivo_bytes":       archivo_bytes,
        "_nombre_archivo":      nombre_archivo,
    }
