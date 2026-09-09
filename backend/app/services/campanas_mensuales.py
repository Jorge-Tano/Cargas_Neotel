"""
Bases PL / REFI — crear y actualizar directamente desde la app
─────────────────────────────────────────────
Antes, la configuración operativa de la base mensual (intentos de
discado, orden discado, teléfonos a discar, contactos a procesar, etc.
— pantalla "Datos de la base de datos" en Neotel) se hacía 100% a mano.

Ahora se puede listar las bases existentes, crear una nueva (`DB_INSERT`)
o actualizar una existente (`DB_UPDATE`) desde acá, para PL o REFI —
disparando una única Tarea de Neotel (Id 83, "Crear/Actualizar Base") vía
`neotel_ws.ejecutar_tarea_con_parametros`: los valores viajan directo
como parámetros SOAP (ExecuteTaskNN), no por archivo — la Tarea decide
internamente, según ClienteCod ("0001"=PL, "0289"=REFI) y TipoOperacion
("CREAR"/"ACTUALIZAR"), qué transacción de base de datos ejecutar. Esto
reemplaza el mecanismo anterior de archivo+FTP+polling (un Datasource
por ECRM); ya no hace falta un Datasource separado para REFI.

Orden de los 15 parámetros (ver Tarea Id 83 en Neotel):
ClienteCod, TipoOperacion, IDDATABASE, DESCRIPCION, ESTADO,
DIFERENCIA_HORARIA, PERMITE_AGREGAR, INTENTOS_DISCADOR, AUTOCERRARCONTACTOS,
INTENTOS_TOTALES, AUTOCERRARCONTACTOS_TOTALES, INTENTOS_DISCADOR_CONTACTO_DIA,
INTENTOS_DISCADOR_CONTACTO_MES, CRM_ORDEN_DISCADO, TELEFONOS_A_DISCAR.

Solo tiene efecto si el usuario logueado es PERDIDAS_USUARIO_AUTORIZADO
(mismo .env que Llamadas Perdidas/Líder Amalia/Opciones de Pago) — para
cualquier otro usuario se lanza un error, no hay una versión "sin tocar
Neotel" para este caso (a diferencia de los demás) porque no genera
ningún archivo de negocio, todo su propósito es escribir en Neotel.
"""
import time
from datetime import date, datetime

from app.core.config import get_settings
from app.core.postgres import registrar_auditoria
from app.core.sqlserver import get_db_name, sqlserver_cursor, obtener_hora_neotel

settings = get_settings()

_TIPOS_VALIDOS = ("PL", "REFI")
_ID_TASK_BASE_CAMPANA = 83
_CLIENTE_COD = {"PL": "0001", "REFI": "0289"}

# Configuración "correcta" confirmada para la base mensual operativa de
# PL/REFI (pantalla "Datos de la base de datos" en Neotel) — la que crea
# sola el flujo ECRM_001 de Neotel viene con otros valores por defecto.
# Usada tanto por el formulario de "Actualizar Base" como por
# app.core.verificador_carga_mensual para detectar y corregir la base
# recién creada cada mes.
PLANTILLA_CORRECTA = {
    "estado": "1",
    "diferencia_horaria": 0,
    "permite_agregar": False,
    "intentos_discador": 100,
    "autocerrar_contactos": False,
    "intentos_totales": 120,
    "autocerrar_contactos_totales": False,
    "intentos_dia": 9999,
    "intentos_mes": 9999,
    "orden_discado": "INTOBM011 DESC, INTOBM011 DESC, INTOBM011 DESC",
    "telefonos_a_discar": "1,2,3",
}


def _validar_tipo(tipo: str) -> str:
    tipo = (tipo or "").upper()
    if tipo not in _TIPOS_VALIDOS:
        raise ValueError(f"tipo debe ser uno de {_TIPOS_VALIDOS}, llegó '{tipo}'")
    return tipo


def _verificar_autorizado(usuario: str):
    if not usuario or not settings.perdidas_usuario_autorizado or usuario != settings.perdidas_usuario_autorizado:
        raise PermissionError("Esta acción escribe directo en Neotel y está restringida.")


def _bool_a_sn(valor) -> str:
    if isinstance(valor, str):
        return "S" if valor.upper() in ("S", "SI", "SÍ", "TRUE", "1") else "N"
    return "S" if valor else "N"


def _parametros_comunes(valores: dict) -> list[str]:
    """Parámetros compartidos entre 'crear' y 'actualizar', en el orden
    declarado en la Tarea 83 (a partir de ESTADO)."""
    return [
        str(valores["estado"]),
        str(valores["diferencia_horaria"]),
        _bool_a_sn(valores["permite_agregar"]),
        str(valores["intentos_discador"]),
        _bool_a_sn(valores["autocerrar_contactos"]),
        str(valores["intentos_totales"]),
        _bool_a_sn(valores["autocerrar_contactos_totales"]),
        str(valores["intentos_dia"]),
        str(valores["intentos_mes"]),
        str(valores["orden_discado"]),
        str(valores["telefonos_a_discar"]),
    ]


def listar_bases(tipo: str, limite: int = 30) -> list[dict]:
    tipo = _validar_tipo(tipo)
    db = get_db_name(tipo)
    linked = settings.sqlserver_linked_host
    query = f"""
        SELECT TOP {int(limite)} IDDATABASE, DESCRIPCION, ESTADO, FHALTA, CONTACTOS
        FROM [{linked}].[{db}].[dbo].[DB]
        WHERE ISNULL(ESTADO, '') <> '6'
        ORDER BY IDDATABASE DESC
    """
    with sqlserver_cursor("master") as cursor:
        cursor.execute(query)
        filas = cursor.fetchall()
    return [
        {
            "iddatabase": int(f[0]),
            "descripcion": f[1],
            "estado": f[2],
            "fhalta": f[3].isoformat() if f[3] else None,
            "contactos": int(f[4]) if f[4] is not None else 0,
        }
        for f in filas
    ]


def obtener_base(tipo: str, iddatabase: int) -> dict | None:
    tipo = _validar_tipo(tipo)
    db = get_db_name(tipo)
    linked = settings.sqlserver_linked_host
    query = f"""
        SELECT IDDATABASE, DESCRIPCION, ESTADO, DIFERENCIA_HORARIA, PERMITE_AGREGAR,
               INTENTOS_DISCADOR, AUTOCERRARCONTACTOS, INTENTOS_TOTALES, AUTOCERRARCONTACTOS_TOTALES,
               INTENTOS_DISCADOR_CONTACTO_DIA, INTENTOS_DISCADOR_CONTACTO_MES,
               CRM_ORDEN_DISCADO, TELEFONOS_A_DISCAR
        FROM [{linked}].[{db}].[dbo].[DB]
        WHERE IDDATABASE = ?
    """
    with sqlserver_cursor("master") as cursor:
        cursor.execute(query, [iddatabase])
        f = cursor.fetchone()
    if not f:
        return None
    return {
        "iddatabase": int(f[0]), "descripcion": f[1], "estado": f[2],
        "diferencia_horaria": f[3], "permite_agregar": f[4] == "S",
        "intentos_discador": f[5], "autocerrar_contactos": f[6] == "S",
        "intentos_totales": f[7], "autocerrar_contactos_totales": f[8] == "S",
        "intentos_dia": f[9], "intentos_mes": f[10],
        "orden_discado": f[11], "telefonos_a_discar": f[12],
    }


def detectar_base_mensual_reciente(tipo: str, desde: date) -> dict | None:
    """
    Busca la base mensual que Neotel crea solo (flujo ECRM_001) para
    `tipo`, identificándola por fecha de creación (FHALTA >= `desde`) —
    NO por nombre: a diferencia del catálogo LEAKAGE, esta base se
    llama "BASE_DD-MM-AAAA", un patrón sin relación con el resto.
    Retorna la de mayor IDDATABASE en ese rango, o None si no hay
    ninguna todavía.
    """
    tipo = _validar_tipo(tipo)
    db = get_db_name(tipo)
    linked = settings.sqlserver_linked_host
    query = f"""
        SELECT TOP 1 IDDATABASE
        FROM [{linked}].[{db}].[dbo].[DB]
        WHERE CONVERT(date, FHALTA) >= ? AND ISNULL(ESTADO, '') <> '6'
        ORDER BY IDDATABASE DESC
    """
    with sqlserver_cursor("master") as cursor:
        cursor.execute(query, [desde])
        row = cursor.fetchone()
    if not row:
        return None
    return obtener_base(tipo, int(row[0]))


def _esperar_base_por_descripcion(tipo: str, descripcion: str, desde: datetime, timeout_seg: int = 40, intervalo_seg: int = 3) -> int | None:
    """Sondea la tabla DB hasta encontrar la base recién creada (la Tarea
    no devuelve el IDDATABASE nuevo, hay que ir a buscarlo)."""
    db = get_db_name(tipo)
    linked = settings.sqlserver_linked_host
    query = f"""
        SELECT TOP 1 IDDATABASE FROM [{linked}].[{db}].[dbo].[DB]
        WHERE DESCRIPCION = ? AND FHALTA >= ?
        ORDER BY IDDATABASE DESC
    """
    limite = time.time() + timeout_seg
    while time.time() < limite:
        with sqlserver_cursor("master") as cursor:
            cursor.execute(query, [descripcion, desde])
            row = cursor.fetchone()
        if row:
            return int(row[0])
        time.sleep(intervalo_seg)
    return None


def crear_base(tipo: str, valores: dict, usuario: str, progress_cb=None) -> dict:
    tipo = _validar_tipo(tipo)
    _verificar_autorizado(usuario)

    def emit(step):
        if progress_cb:
            progress_cb(step)

    from app.core.neotel_ws import ejecutar_tarea_con_parametros

    emit("Consultando hora de Neotel")
    hora_disparo = obtener_hora_neotel() or datetime.now()

    emit("Disparando creación de base en Neotel")
    parametros = [_CLIENTE_COD[tipo], "CREAR", "", str(valores["descripcion"])] + _parametros_comunes(valores)
    resultado_tarea = ejecutar_tarea_con_parametros(_ID_TASK_BASE_CAMPANA, parametros)
    if resultado_tarea is None or resultado_tarea.startswith("ERROR"):
        raise RuntimeError(resultado_tarea or "No se pudo disparar la creación de la base en Neotel")

    emit("Esperando que la base aparezca en Neotel")
    nuevo_id = _esperar_base_por_descripcion(tipo, valores["descripcion"], hora_disparo)
    if nuevo_id is None:
        raise RuntimeError("La base no se pudo confirmar tras crearla (revisar en Neotel)")

    registrar_auditoria(
        usuario, f"CREAR_BASE_{tipo}",
        f"IDDATABASE={nuevo_id} DESCRIPCION={valores['descripcion']!r} valores={valores!r}",
    )

    emit("Listo")
    return {"iddatabase": nuevo_id, "descripcion": valores["descripcion"]}


def actualizar_base(
    tipo: str, iddatabase: int, valores: dict, usuario: str, progress_cb=None,
    nueva_descripcion: str | None = None, omitir_verificacion: bool = False,
) -> dict:
    """
    `nueva_descripcion`: si viene, renombra la base (ej. el verificador
    mensual de REFI la deja como "BASE RN {MES} {AÑO}"); si no, se
    mantiene la Descripción actual tal cual. `omitir_verificacion`: para
    el propio backend (ver core.verificador_carga_mensual) — el chequeo
    de PERDIDAS_USUARIO_AUTORIZADO es para pedidos de un usuario real vía
    API, no aplica a la corrección automática que corre nuestro sistema.
    """
    tipo = _validar_tipo(tipo)
    if not omitir_verificacion:
        _verificar_autorizado(usuario)

    def emit(step):
        if progress_cb:
            progress_cb(step)

    from app.core.neotel_ws import ejecutar_tarea_con_parametros

    if nueva_descripcion:
        descripcion = nueva_descripcion
    else:
        actual = obtener_base(tipo, iddatabase)
        if not actual:
            raise RuntimeError(f"No existe la base IDDATABASE={iddatabase}")
        descripcion = actual["descripcion"]

    emit("Disparando actualización de base en Neotel")
    parametros = [_CLIENTE_COD[tipo], "ACTUALIZAR", str(iddatabase), descripcion] + _parametros_comunes(valores)
    resultado_tarea = ejecutar_tarea_con_parametros(_ID_TASK_BASE_CAMPANA, parametros)
    if resultado_tarea is None or resultado_tarea.startswith("ERROR"):
        raise RuntimeError(resultado_tarea or "No se pudo disparar la actualización de la base en Neotel")

    emit("Confirmando cambios")
    time.sleep(3)
    base = obtener_base(tipo, iddatabase)
    if not base or str(base.get("estado")) != str(valores["estado"]):
        raise RuntimeError("No se pudo confirmar que la base haya quedado actualizada")

    registrar_auditoria(
        usuario, f"ACTUALIZAR_BASE_{tipo}",
        f"IDDATABASE={iddatabase} descripcion={descripcion!r} valores={valores!r}",
    )

    emit("Listo")
    return base


def ejecutar_resultante(tipo: str, iddatabase: int) -> bool:
    """
    Dispara el export "Resultante" (FTP) de `tipo` para `iddatabase` —
    una única Tarea de Neotel (separada de la 83) con dispatch por
    ClienteCod, igual que Crear/Actualizar Base: recibe (ClienteCod,
    BASE), arma fechas, vuelca un StoredProcedure a CSV vía nodo
    "Archivo Customizado" (Datasource/Conexión según el tipo) y borra
    el archivo del mes anterior.

    El Id de esa Tarea se guarda en config_global (ID_TAREA_RESULTANTE)
    porque se crea directo en Neotel, no hay forma de descubrirlo por
    código — mientras no esté configurado, no hace nada y retorna False
    sin lanzar excepción.
    """
    tipo = _validar_tipo(tipo)
    from app.core.postgres import get_config_valor
    id_tarea = get_config_valor("ID_TAREA_RESULTANTE")
    if not id_tarea:
        return False

    from app.core.neotel_ws import ejecutar_tarea_con_parametros
    resultado = ejecutar_tarea_con_parametros(int(id_tarea), [_CLIENTE_COD[tipo], str(iddatabase)])
    return resultado is not None and not resultado.startswith("ERROR")
