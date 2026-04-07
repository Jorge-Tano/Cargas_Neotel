"""
Backend FastAPI - Neotel Cargas
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from datetime import date
from app.core.auth import (
    LoginRequest, TokenResponse,
    autenticar_ad, crear_token, verificar_token,
)
from app.core.postgres import (
    registrar_auditoria, get_auditoria, get_repetidos_log,
    get_config_global, set_config_global,
    get_config_usuario, set_config_usuario, get_config_valor,
)
import os, json, uuid, time, queue as _queue, asyncio
from concurrent.futures import ThreadPoolExecutor

app = FastAPI(title="Neotel Cargas API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

_executor = ThreadPoolExecutor(max_workers=8)
_jobs: dict[str, dict] = {}

TIPOS_CASO = ["SAV", "AV", "REFI", "PL", "PERDIDAS"]

MESES = {
    1:"01-Enero",  2:"02-Febrero", 3:"03-Marzo",      4:"04-Abril",
    5:"05-Mayo",   6:"06-Junio",   7:"07-Julio",       8:"08-Agosto",
    9:"09-Septiembre", 10:"10-Octubre", 11:"11-Noviembre", 12:"12-Diciembre",
}

# =============================================================
# JOB SYSTEM (SSE)
# =============================================================

def _create_job() -> str:
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"q": _queue.Queue(), "done": False}
    return job_id

def _emit(job_id: str, step: str, elapsed: float, done: bool = False,
          result: dict = None, error: str = None):
    if job_id not in _jobs:
        return
    _jobs[job_id]["q"].put({
        "step": step, "elapsed": round(elapsed, 1), "done": done,
        **({"result": result} if result else {}),
        **({"error": error} if error else {}),
    })
    if done:
        _jobs[job_id]["done"] = True

@app.get("/jobs/{job_id}/stream")
async def stream_job(job_id: str):
    async def generate():
        if job_id not in _jobs:
            yield f"data: {json.dumps({'error': 'Job no encontrado', 'done': True})}\n\n"
            return
        q = _jobs[job_id]["q"]
        while True:
            try:
                msg = q.get_nowait()
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("done"):
                    _jobs.pop(job_id, None)
                    break
            except _queue.Empty:
                yield ": keepalive\n\n"
                await asyncio.sleep(0.2)

    return StreamingResponse(generate(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    })

# =============================================================
# AUTH
# =============================================================

@app.post("/auth/login", response_model=TokenResponse)
def login(body: LoginRequest):
    info = autenticar_ad(body.usuario, body.password)
    if not info:
        raise HTTPException(status_code=401, detail="Usuario o contraseña incorrectos")
    return TokenResponse(access_token=crear_token(info), nombre=info["nombre"])

@app.get("/auth/me")
def me(user: dict = Depends(verificar_token)):
    return user

# =============================================================
# HELPERS DE RUTAS
# =============================================================

def _build_path(ruta_base: str, con_dia: bool = True) -> str:
    hoy = date.today()
    partes = [ruta_base, str(hoy.year), MESES[hoy.month]]
    if con_dia:
        partes.append(f"{hoy.day:02d}")
    return os.path.join(*partes)

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
        print(f"Carpeta {'existente' if os.path.exists(path) else 'creada'}: {path}")
    except Exception as e:
        print(f"No se pudo crear carpeta {path}: {e}")

def get_output_dirs(tipo: str, usuario: str = "") -> dict:
    dirs = {}
    con_dia = tipo != "PERDIDAS"
    try:
        u_cfg = get_config_usuario(usuario).get(tipo, {}) if usuario else {}
        if u_cfg.get("guardar_compartida", True):
            ruta_c = get_config_global().get(f"ruta_{tipo.lower()}_compartida", "")
            if ruta_c:
                path_c = _build_path(ruta_c, con_dia=con_dia)
                _ensure_dir(path_c)
                dirs["compartida"] = path_c
        if u_cfg.get("guardar_local") and u_cfg.get("ruta_local"):
            path_l = u_cfg["ruta_local"]
            _ensure_dir(path_l)
            dirs["local"] = path_l
    except Exception:
        pass
    return dirs

def get_output_dir(tipo: str, usuario: str = "") -> str:
    dirs = get_output_dirs(tipo, usuario)
    return dirs.get("compartida") or dirs.get("local") or "/tmp"

def _archivos_generados(resultado: dict) -> list:
    return [
        {"nombre": os.path.basename(path), "path": path}
        for key, path in resultado.items()
        if key.startswith("archivo") and path and os.path.exists(path)
    ]

def _copiar_archivo_base(archivo_bytes: bytes, nombre: str, tipo: str, usuario: str = ""):
    for variante, carpeta in get_output_dirs(tipo, usuario).items():
        try:
            with open(os.path.join(carpeta, nombre), "wb") as f:
                f.write(archivo_bytes)
            print(f"Archivo base copiado [{variante}]: {carpeta}/{nombre}")
        except Exception as e:
            print(f"No se pudo copiar archivo base [{variante}]: {e}")

# =============================================================
# ENDPOINTS DE PROCESO
# =============================================================

def _run_sav_av(tipo: str, contenido: bytes | None, nombre: str | None,
                usuario: str, job_id: str, t0: float):
    from app.services.sav_av import procesar_sav_av
    try:
        resultado = procesar_sav_av(contenido, nombre, tipo, get_output_dir(tipo, usuario),
                                    progress_cb=lambda s: _emit(job_id, s, time.time() - t0),
                                    usuario=usuario)
        archivo_bytes  = resultado.pop("_archivo_bytes", None)
        nombre_archivo = resultado.pop("_nombre_archivo", None)
        resultado["archivos"] = _archivos_generados(resultado)
        if archivo_bytes and nombre_archivo:
            _copiar_archivo_base(archivo_bytes, nombre_archivo, tipo, usuario)
        _emit(job_id, "Completado", time.time() - t0, done=True, result=resultado)
    except Exception as e:
        _emit(job_id, str(e), time.time() - t0, done=True, error=str(e))

def _run_refi_pl(tipo: str, usuario: str, job_id: str, t0: float):
    from app.services.refi_pl import procesar_refi_pl
    try:
        resultado = procesar_refi_pl(tipo=tipo, output_dir=get_output_dir(tipo, usuario),
                                     progress_cb=lambda s: _emit(job_id, s, time.time() - t0),
                                     usuario=usuario)
        archivo_bytes  = resultado.pop("_archivo_bytes", None)
        nombre_archivo = resultado.pop("_nombre_archivo", None)
        resultado["archivos"] = _archivos_generados(resultado)
        if archivo_bytes and nombre_archivo:
            _copiar_archivo_base(archivo_bytes, nombre_archivo, tipo, usuario)
        _emit(job_id, "Completado", time.time() - t0, done=True, result=resultado)
    except Exception as e:
        _emit(job_id, str(e), time.time() - t0, done=True, error=str(e))

@app.post("/procesar/sav", dependencies=[Depends(verificar_token)])
async def procesar_sav(file: UploadFile = File(None), user: dict = Depends(verificar_token)):
    contenido, nombre = (await file.read(), file.filename) if file else (None, None)
    job_id, t0 = _create_job(), time.time()
    _executor.submit(_run_sav_av, "SAV", contenido, nombre, user.get("usuario", ""), job_id, t0)
    return {"job_id": job_id}

@app.post("/procesar/av", dependencies=[Depends(verificar_token)])
async def procesar_av(file: UploadFile = File(None), user: dict = Depends(verificar_token)):
    contenido, nombre = (await file.read(), file.filename) if file else (None, None)
    job_id, t0 = _create_job(), time.time()
    _executor.submit(_run_sav_av, "AV", contenido, nombre, user.get("usuario", ""), job_id, t0)
    return {"job_id": job_id}

@app.post("/procesar/refi", dependencies=[Depends(verificar_token)])
async def procesar_refi(user: dict = Depends(verificar_token)):
    job_id, t0 = _create_job(), time.time()
    _executor.submit(_run_refi_pl, "REFI", user.get("usuario", ""), job_id, t0)
    return {"job_id": job_id}

@app.post("/procesar/pl", dependencies=[Depends(verificar_token)])
async def procesar_pl(user: dict = Depends(verificar_token)):
    job_id, t0 = _create_job(), time.time()
    _executor.submit(_run_refi_pl, "PL", user.get("usuario", ""), job_id, t0)
    return {"job_id": job_id}

@app.post("/procesar/perdidas", dependencies=[Depends(verificar_token)])
async def procesar_perdidas(file: UploadFile = File(...), user: dict = Depends(verificar_token)):
    from app.services.perdidas import procesar_llamadas_perdidas
    contenido, nombre, usuario = await file.read(), file.filename, user.get("usuario", "")
    job_id, t0 = _create_job(), time.time()
    def run():
        try:
            resultado = procesar_llamadas_perdidas(contenido, nombre,
                                                   get_output_dir("PERDIDAS", usuario),
                                                   progress_cb=lambda s: _emit(job_id, s, time.time() - t0),
                                                   usuario=usuario)
            resultado["archivos"] = _archivos_generados(resultado)
            _copiar_archivo_base(contenido, nombre, "PERDIDAS", usuario)
            _emit(job_id, "Completado", time.time() - t0, done=True, result=resultado)
        except Exception as e:
            _emit(job_id, str(e), time.time() - t0, done=True, error=str(e))
    _executor.submit(run)
    return {"job_id": job_id}

# =============================================================
# LISTA NEGRA
# =============================================================

@app.post("/lista-negra/actualizar", dependencies=[Depends(verificar_token)])
async def actualizar_lista_negra(file: UploadFile = File(None), user: dict = Depends(verificar_token)):
    from app.services.lista_negra import procesar_lista_negra
    usuario = user.get("usuario", "")
    try:
        resultado = procesar_lista_negra(
            **({"archivo_bytes": await file.read(), "nombre_archivo": file.filename} if file else {})
        )
        if any(resultado.get(k, 0) > 0 for k in ("insertados", "actualizados", "eliminados")):
            registrar_auditoria(
                usuario=usuario, accion="Lista Negra actualizada",
                detalle=(f"Insertados: {resultado.get('insertados',0)}, "
                         f"Actualizados: {resultado.get('actualizados',0)}, "
                         f"Eliminados: {resultado.get('eliminados',0)}, "
                         f"Total activos: {resultado.get('total_activos',0)}")
            )
        return resultado
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/lista-negra/total", dependencies=[Depends(verificar_token)])
async def get_total_lista_negra():
    from app.core.postgres import get_total_lista_negra
    return get_total_lista_negra()

# =============================================================
# LOGS / AUDITORÍA
# =============================================================

@app.get("/logs", dependencies=[Depends(verificar_token)])
async def get_logs(limit: int = 50):
    from app.core.postgres import get_logs
    return get_logs(limit=limit)

@app.get("/auditoria", dependencies=[Depends(verificar_token)])
async def get_auditoria_endpoint(limit: int = 100):
    return get_auditoria(limit=limit)

@app.get("/repetidos", dependencies=[Depends(verificar_token)])
async def get_repetidos_endpoint(tipo_caso: str = None, limit: int = 200):
    return get_repetidos_log(tipo_caso=tipo_caso, limit=limit)

@app.get("/consulta-repetidos", dependencies=[Depends(verificar_token)])
async def consultar_repetidos_live(caso: str):
    from app.core.sqlserver import get_repetidos
    caso = caso.upper()
    if caso not in ("SAV", "AV", "REFI", "PL", "SAV_AV"):
        raise HTTPException(status_code=400, detail=f"Caso '{caso}' no válido.")
    try:
        caso_bd = "SAV_AV" if caso in ("SAV", "AV") else caso
        ruts = get_repetidos(caso_bd)
        return {"caso": caso, "total": len(ruts), "ruts": sorted(ruts)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================
# CONFIG
# =============================================================

@app.get("/config/general", dependencies=[Depends(verificar_token)])
async def get_config_general(user: dict = Depends(verificar_token)):
    usuario = user.get("usuario", "")
    try:
        cfg_u = get_config_usuario(usuario) if usuario else {}
    except Exception:
        cfg_u = {}
    return {
        "guardar_local":      any(v.get("guardar_local",      False) for v in cfg_u.values()),
        "guardar_compartida": any(v.get("guardar_compartida", True)  for v in cfg_u.values()),
    }

@app.put("/config/general", dependencies=[Depends(verificar_token)])
async def set_config_general(body: dict, user: dict = Depends(verificar_token)):
    usuario = user.get("usuario", "")
    cfg_u   = get_config_usuario(usuario) if usuario else {}
    cambios = []

    for tipo in TIPOS_CASO:
        u             = cfg_u.get(tipo, {})
        ruta_local    = u.get("ruta_local", "")
        guardar_local = u.get("guardar_local", False)
        guardar_comp  = u.get("guardar_compartida", True)
        actualizar    = False

        if "guardar_local" in body:
            nuevo = bool(body["guardar_local"])
            if guardar_local != nuevo:
                guardar_local = nuevo
                actualizar = True
        if "guardar_compartida" in body:
            nuevo = bool(body["guardar_compartida"])
            if guardar_comp != nuevo:
                guardar_comp = nuevo
                actualizar = True

        if actualizar:
            set_config_usuario(usuario, tipo, ruta_local, guardar_local, guardar_comp)
            cambios.append(tipo)

    if cambios:
        registrar_auditoria(usuario=usuario, accion="Config general actualizada",
                            detalle=f"Tipos actualizados: {', '.join(cambios)}")
    return await get_config_general(user)

@app.get("/config/rutas", dependencies=[Depends(verificar_token)])
async def get_rutas(user: dict = Depends(verificar_token)):
    usuario = user.get("usuario", "")
    cfg     = get_config_global()
    cfg_u   = get_config_usuario(usuario) if usuario else {}
    return {
        **{f"ruta_{t.lower()}_compartida": cfg.get(f"ruta_{t.lower()}_compartida", "")  for t in TIPOS_CASO},
        **{f"ruta_{t.lower()}_local":      cfg_u.get(t, {}).get("ruta_local", "")        for t in TIPOS_CASO},
        **{f"guardar_local_{t.lower()}":   cfg_u.get(t, {}).get("guardar_local", False)   for t in TIPOS_CASO},
        "ruta_blacklist_red": cfg.get("ruta_blacklist_red", ""),
    }

@app.put("/config/rutas", dependencies=[Depends(verificar_token)])
async def set_rutas(body: dict, user: dict = Depends(verificar_token)):
    usuario     = user.get("usuario", "")
    cfg_antes   = get_config_global()
    cfg_u_antes = get_config_usuario(usuario) if usuario else {}
    cambios_global, cambios_log = {}, []

    for tipo in TIPOS_CASO:
        key_c = f"ruta_{tipo.lower()}_compartida"
        if key_c in body:
            nuevo = body[key_c].strip()
            if cfg_antes.get(key_c, "") != nuevo:
                cambios_global[key_c] = nuevo
                cambios_log.append(f"{key_c}: {cfg_antes.get(key_c)} → {nuevo}")
        key_l = f"ruta_{tipo.lower()}_local"
        key_g = f"guardar_local_{tipo.lower()}"
        if key_l in body or key_g in body:
            u             = cfg_u_antes.get(tipo, {})
            nueva_ruta    = body.get(key_l, u.get("ruta_local", "")).strip()
            nuevo_guardar = bool(body.get(key_g, u.get("guardar_local", False)))
            guardar_comp  = u.get("guardar_compartida", True)
            if u.get("ruta_local") != nueva_ruta or u.get("guardar_local") != nuevo_guardar:
                set_config_usuario(usuario, tipo, nueva_ruta, nuevo_guardar, guardar_comp)
                cambios_log.append(f"{key_l}({usuario}): → {nueva_ruta}")

    if "ruta_blacklist_red" in body:
        nuevo = body["ruta_blacklist_red"].strip()
        if cfg_antes.get("ruta_blacklist_red", "") != nuevo:
            cambios_global["ruta_blacklist_red"] = nuevo
            cambios_log.append(f"ruta_blacklist_red: → {nuevo}")

    if cambios_global:
        set_config_global(cambios_global)
    if cambios_log:
        registrar_auditoria(usuario=usuario, accion="Rutas actualizadas",
                            detalle=", ".join(cambios_log))
    return await get_rutas(user)

@app.get("/config/iddatabase", dependencies=[Depends(verificar_token)])
async def get_iddatabase():
    cfg = get_config_global()
    return {k: cfg.get(k) for k in [
        "IDDATABASE_SAV", "IDDATABASE_AV", "IDDATABASE_PL", "IDDATABASE_REFI",
        "DB_SAV_AV", "DB_AV", "DB_PL", "DB_REFI",
    ]}

@app.put("/config/iddatabase", dependencies=[Depends(verificar_token)])
async def set_iddatabase(body: dict, user: dict = Depends(verificar_token)):
    cfg_antes = get_config_global()
    datos = {
        **{c: str(int(body[c])) for c in ["IDDATABASE_SAV","IDDATABASE_AV","IDDATABASE_PL","IDDATABASE_REFI"] if c in body},
        **{c: str(body[c]).strip() for c in ["DB_SAV_AV","DB_AV","DB_PL","DB_REFI"] if c in body},
    }
    cambios = {k: v for k, v in datos.items() if cfg_antes.get(k) != v}
    if cambios:
        set_config_global(cambios)
        registrar_auditoria(usuario=user.get("usuario", ""), accion="IDs de BD actualizados",
                            detalle=", ".join(f"{k}: {cfg_antes.get(k)} → {v}" for k, v in cambios.items()))
    return await get_iddatabase()

@app.get("/config/sftp", dependencies=[Depends(verificar_token)])
async def get_sftp():
    cfg = get_config_global()
    return {k: cfg.get(k, "") for k in [
        "sftp_keyword_global",
        "sftp_keyword_SAV", "sftp_keyword_AV", "sftp_keyword_REFI", "sftp_keyword_PL",
    ]}

@app.put("/config/sftp", dependencies=[Depends(verificar_token)])
async def set_sftp(body: dict, user: dict = Depends(verificar_token)):
    cfg_antes = get_config_global()
    campos    = [
        "sftp_keyword_global",
        "sftp_keyword_SAV", "sftp_keyword_AV", "sftp_keyword_REFI", "sftp_keyword_PL",
    ]
    datos   = {c: str(body[c]).strip() for c in campos if c in body}
    cambios = {k: v for k, v in datos.items() if cfg_antes.get(k) != v}
    if cambios:
        set_config_global(cambios)
        registrar_auditoria(usuario=user.get("usuario", ""), accion="Config SFTP actualizada",
                            detalle=", ".join(cambios.keys()))
    return await get_sftp()

# =============================================================
# MISC
# =============================================================

@app.get("/descargar", dependencies=[Depends(verificar_token)])
async def descargar_archivo(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(path, filename=os.path.basename(path))

@app.get("/health")
async def health():
    try:
        cfg = get_config_global()
        return {
            "status": "ok",
            "guardar_compartida": cfg.get("guardar_compartida", "false") == "true",
            "rutas": {t: cfg.get(f"ruta_{t.lower()}_compartida", "") for t in TIPOS_CASO},
        }
    except Exception:
        return {"status": "ok"}