"""
Caso: Líder Amalia
─────────────────────────────────────────────
Entrada: xlsx con una columna de teléfono en formato internacional
  (ej. "numero_celular" = "56998296895") — puede venir con nombre de
  columna variable, se detecta por contener "cel"/"tel"/"fono".

Proceso:
  1. Leer archivo.
  2. Reconstruir el teléfono a formato local de marcado (0XXXXXXXXX).
  3. Generar Carga: Telefono1, FechaCarga (d/m/aaaa, sin cero adelante
     en el día — igual al formato real ya usado en LoteBddAmalia*.xls),
     OrdenDiscado (siempre 99999).

Salida: LoteBddAmalia{fecha}.xls (mismo layout que el archivo que ya se
generaba a mano).

Subida a Neotel: TXT con el mismo layout, sube a /UPLOAD/cargas_manual,
dispara ExecuteTask00 y confirma vía LOTES — igual que Llamadas
Perdidas. Destino: ECRM_0059, IDDATABASE rota mes a mes (catálogo real
"BDD AMALIA {MES} {AÑO}", detectado automático — ver
app.core.verificador_iddatabase / sqlserver.detectar_base_reciente).

La subida a Neotel solo ocurre si el usuario logueado es
PERDIDAS_USUARIO_AUTORIZADO (mismo .env que Llamadas Perdidas) —
cualquier otro usuario sigue viendo solo el Excel generado, sin tocar
Neotel (y sin ver esta tarjeta en la UI, ver page.tsx CASOS_SOLO_JORGE).
"""

import re
import pandas as pd
from datetime import date, datetime

from app.core.postgres import registrar_log
from app.services.utils import agregar_cero, exportar_multi_destino, exportar_txt_carga, extraer_horario_archivo, leer_archivo

COLUMNAS_SALIDA = ["Telefono1", "FechaCarga", "OrdenDiscado"]

_RE_TEL_INTL = re.compile(r"^\+?56(\d{9})$")


def _reconstruir_telefono(telefono_raw: str) -> str:
    """"56998296895" -> "0998296895". Si ya viene en formato local
    (9 dígitos) o cualquier otro formato no reconocido, se deja tal cual."""
    t = (telefono_raw or "").strip().replace(" ", "")
    m = _RE_TEL_INTL.match(t)
    if m:
        return "0" + m.group(1)
    if len(t) == 9 and t.isdigit():
        return "0" + t
    return t


def _fecha_carga_hoy() -> str:
    """d/m/aaaa sin cero adelante en el día (ej. "4/09/2026"), igual al
    formato real que ya usaba el archivo generado a mano."""
    hoy = date.today()
    return f"{hoy.day}/{hoy.month:02d}/{hoy.year}"


def _col(df: pd.DataFrame, col: str, default: str = "") -> list:
    if col in df.columns:
        return df[col].fillna("").astype(str).tolist()
    return [default] * len(df)


def procesar_lider_amalia(
    archivo_bytes: bytes,
    nombre_archivo: str,
    output_dirs: dict = None,
    progress_cb=None,
    usuario: str = "",
    procesar_neotel: bool = True,
) -> dict:
    def emit(step):
        if progress_cb:
            progress_cb(step)

    output_dirs = output_dirs or {}
    hoy_compacto = date.today().strftime("%Y%m%d")

    # 1. Leer archivo
    emit("Leyendo archivo")
    df = leer_archivo(archivo_bytes, nombre_archivo)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)

    # 2. Identificar columna de teléfono
    emit("Identificando columna de teléfono")
    col_telefono = None
    for col in df.columns:
        if "cel" in col.lower() or "tel" in col.lower() or "fono" in col.lower():
            col_telefono = col
            break
    if col_telefono is None:
        raise ValueError("No se encontró columna de teléfono en el archivo de entrada.")

    # 3. Construir Carga
    emit("Construyendo archivo de carga")
    n = len(df)
    telefonos = [_reconstruir_telefono(v) for v in _col(df, col_telefono)]
    df_carga = pd.DataFrame({
        "Telefono1":    [agregar_cero(t) for t in telefonos],
        "FechaCarga":   [_fecha_carga_hoy()] * n,
        "OrdenDiscado": [99999] * n,
    })

    # 3b. Separar registros sin teléfono válido
    emit("Separando registros sin teléfono")
    mask_sin_telefono = df_carga["Telefono1"].astype(str).str.strip().isin(["", "00"])
    df_sin_telefono = df_carga[mask_sin_telefono].reset_index(drop=True)
    df_carga        = df_carga[~mask_sin_telefono].reset_index(drop=True)

    # 4. Generar TXT y subir por FTP a Neotel + disparar import — solo si
    #    procesar_neotel viene en True (ya decidido según permisos por
    #    usuario antes de llegar acá, ver main._neotel_efectivo).
    carga_forzada = False
    hora_disparo = None
    path_carga_txt = None
    if len(df_carga) > 0 and procesar_neotel:
        emit("Generando archivo de carga en TXT para Neotel")
        carpeta_txt = output_dirs.get("compartida") or output_dirs.get("local") or "/tmp"
        horario_txt = extraer_horario_archivo(nombre_archivo or "") or datetime.now().strftime("%H%M")
        nombre_carga_txt = f"SalidaLiderAmalia{hoy_compacto}{horario_txt}.txt"
        path_carga_txt = exportar_txt_carga(df_carga, f"{carpeta_txt}/{nombre_carga_txt}", COLUMNAS_SALIDA)

        if path_carga_txt:
            emit("Subiendo TXT de carga por FTP a Neotel")
            try:
                from app.core.ftp_neotel17 import subir_archivo_carga_txt
                subir_archivo_carga_txt(path_carga_txt, tipo="AMALIA")
            except Exception as e:
                print(f"⚠️  Error subiendo TXT por FTP: {e}")

            emit("Disparando import inmediato en Neotel")
            try:
                from app.core.sqlserver import obtener_hora_neotel
                from app.core.neotel_ws import ejecutar_tarea
                hora_disparo = obtener_hora_neotel()
                carga_forzada = ejecutar_tarea("AMALIA") is not None
            except Exception as e:
                print(f"⚠️  Error disparando import en Neotel: {e}")

        try:
            from app.core.confirmacion_carga import confirmar_carga_en_segundo_plano
            confirmar_carga_en_segundo_plano(
                caso="AMALIA",
                valores=df_carga["Telefono1"].astype(str).tolist(),
                columna="TELTELEFONO1",
                archivo_origen=nombre_archivo,
                usuario=usuario,
                carga_forzada=carga_forzada,
                hora_disparo=hora_disparo,
            )
        except Exception as e:
            print(f"⚠️  Error iniciando confirmación de carga AMALIA en Neotel: {e}")

    # 5. Exportar Excel: Carga va a compartida y local; Sin Teléfono solo a compartida
    emit("Generando archivo Excel")
    nombre_carga       = f"LoteBddAmalia{hoy_compacto}.xls"
    nombre_sin_telefono = f"SinTelefonoLiderAmalia{hoy_compacto}.xls"
    tareas = [
        (df_carga,        nombre_carga,        "Contactos", True,  "carga"),
        (df_sin_telefono, nombre_sin_telefono, "Contactos", False, "sin_telefono"),
    ]
    paths = exportar_multi_destino(tareas, output_dirs, claves_local={"carga"})
    path_carga        = paths["carga"]
    path_sin_telefono = paths["sin_telefono"]

    # 6. Log
    registrar_log(
        tipo_caso="AMALIA",
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
