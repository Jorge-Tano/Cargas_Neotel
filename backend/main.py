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
from app.core.postgres import registrar_auditoria, get_auditoria
import os, json, uuid, time, queue as _queue, asyncio
from concurrent.futures import ThreadPoolExecutor

app = FastAPI(title="Neotel Cargas API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,
)

# ── Pool de hilos para procesar en paralelo ──────────────────
_executor = ThreadPoolExecutor(max_workers=8)

# ── Job system para SSE ──────────────────────────────────────
_jobs: dict[str, dict] = {}

def _create_job() -> str:
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"q": _queue.Queue(), "done": False}
    return job_id

def _emit(job_id: str, step: str, elapsed: float, done: bool = False, result: dict = None, error: str = None):
    if job_id not in _jobs:
        return
    _jobs[job_id]["q"].put({
        "step": step, "elapsed": round(elapsed, 1),
        "done": done,
        **({"result": result} if result else {}),
        **({"error": error} if error else {}),
    })
    if done:
        _jobs[job_id]["done"] = True

app = FastAPI(title="Neotel Cargas API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,
)

# =============================================================
# AUTH ENDPOINTS (publicos)
# =============================================================
@app.post("/auth/login", response_model=TokenResponse)
def login(body: LoginRequest):
    info = autenticar_ad(body.usuario, body.password)
    if not info:
        raise HTTPException(status_code=401, detail="Usuario o contrasena incorrectos")
    return TokenResponse(access_token=crear_token(info), nombre=info["nombre"])

@app.get("/auth/me")
def me(user: dict = Depends(verificar_token)):
    return user

# =============================================================
# SSE — streaming de progreso por job
# =============================================================
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
# CONFIG
# =============================================================
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

MESES = {
    1:"01-Enero", 2:"02-Febrero", 3:"03-Marzo", 4:"04-Abril",
    5:"05-Mayo", 6:"06-Junio", 7:"07-Julio", 8:"08-Agosto",
    9:"09-Septiembre", 10:"10-Octubre", 11:"11-Noviembre", 12:"12-Diciembre"
}

RUTAS_COMPARTIDA_DEFAULT = {
    "SAV":      r"\\10.0.1.40\informatica\Neotel\Presto\Leakage\SAV Leakage",
    "AV":       r"\\10.0.1.40\informatica\Neotel\Presto\Leakage\Avance Leakage",
    "REFI":     r"\\10.0.1.40\informatica\Neotel\Presto\REFI LEAKAGE\CARGAS",
    "PL":       r"\\10.0.1.40\informatica\Neotel\Presto\PL LEAKAGE\CARGAS",
    "PERDIDAS": r"\\10.0.1.40\informatica\Neotel\Presto\Seguimiento PPFF",
}

RUTAS_LOCAL_DEFAULT = {
    "SAV":      r"C:\Cargas",
    "AV":       r"C:\Cargas",
    "REFI":     r"C:\Cargas",
    "PL":       r"C:\Cargas",
    "PERDIDAS": r"C:\Cargas",
}

# Subcarpetas que se añaden a la ruta local configurada por el usuario
SUBCARPETAS_LOCAL = {
    "SAV":      r"Cargas_Neotel-Leakage\SAV Leakage",
    "AV":       r"Cargas_Neotel-Leakage\Avance Leakage",
    "REFI":     r"Cargas_Neotel-Leakage\REFI Leakage",
    "PL":       r"Cargas_Neotel-Leakage\PL Leakage",
    "PERDIDAS": r"Cargas_Neotel-Seguimiento PPFF",
}

def _leer_config() -> dict:
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def _guardar_config(cfg: dict):
    existing = _leer_config()
    existing.update(cfg)
    with open(CONFIG_PATH, "w") as f:
        json.dump(existing, f, indent=2)

def guardar_local_activo() -> bool:
    return _leer_config().get("guardar_local", False)

def guardar_compartida_activo() -> bool:
    return _leer_config().get("guardar_compartida", True)

def get_ruta_caso(tipo: str, variante: str = "compartida") -> str:
    cfg = _leer_config()
    key = f"ruta_{tipo.lower()}_{variante}"
    defaults = RUTAS_COMPARTIDA_DEFAULT if variante == "compartida" else RUTAS_LOCAL_DEFAULT
    return cfg.get(key, defaults.get(tipo, r"C:\Cargas"))

def _build_path(ruta_base: str, con_dia: bool = True) -> str:
    hoy = date.today()
    if con_dia:
        return os.path.join(ruta_base, str(hoy.year), MESES[hoy.month], f"{hoy.day:02d}")
    return os.path.join(ruta_base, str(hoy.year), MESES[hoy.month])

def _ensure_dir(path: str):
    if os.path.exists(path):
        print(f"Carpeta existente: {path}")
    else:
        try:
            os.makedirs(path, exist_ok=True)
            print(f"Carpeta creada: {path}")
        except Exception as e:
            print(f"No se pudo crear carpeta {path}: {e}")

def get_output_dirs(tipo: str) -> dict:
    dirs = {}
    con_dia = tipo != "PERDIDAS"
    if guardar_compartida_activo():
        path_compartida = _build_path(get_ruta_caso(tipo, "compartida"), con_dia=con_dia)
        _ensure_dir(path_compartida)
        dirs["compartida"] = path_compartida
    if guardar_local_activo():
        ruta_local_base = get_ruta_caso(tipo, "local").strip()
        if ruta_local_base:
            subcarpeta = SUBCARPETAS_LOCAL.get(tipo, "")
            ruta_local = os.path.join(ruta_local_base, subcarpeta) if subcarpeta else ruta_local_base
            path_local = _build_path(ruta_local, con_dia=con_dia)
            _ensure_dir(path_local)
            dirs["local"] = path_local
    return dirs

def get_output_dir(tipo: str) -> str:
    dirs = get_output_dirs(tipo)
    return dirs.get("compartida") or dirs.get("local") or "/tmp"

def _archivos_generados(resultado: dict) -> list:
    archivos = []
    for key, path in resultado.items():
        if key.startswith("archivo") and isinstance(path, str) and path and os.path.exists(path):
            archivos.append({"nombre": os.path.basename(path), "path": path})
    return archivos

def _copiar_archivo_base(archivo_bytes: bytes, nombre: str, tipo: str):
    dirs = get_output_dirs(tipo)
    for variante, carpeta in dirs.items():
        try:
            dest = os.path.join(carpeta, nombre)
            with open(dest, "wb") as f:
                f.write(archivo_bytes)
            print(f"Archivo base copiado [{variante}]: {dest}")
        except Exception as e:
            print(f"No se pudo copiar archivo base [{variante}]: {e}")

def _copiar_archivos_procesados_a_local(resultado: dict, tipo: str):
    """Copia todos los archivos procesados (carga, repetidos, bloqueo) a la carpeta local."""
    if not guardar_local_activo():
        return
    dirs = get_output_dirs(tipo)
    carpeta_local = dirs.get("local")
    if not carpeta_local:
        return
    import shutil
    for key, path in resultado.items():
        if key.startswith("archivo") and isinstance(path, str) and path and os.path.exists(path):
            try:
                dest = os.path.join(carpeta_local, os.path.basename(path))
                shutil.copy2(path, dest)
                print(f"Archivo procesado copiado [local]: {dest}")
            except Exception as e:
                print(f"No se pudo copiar archivo procesado [local] {path}: {e}")

# =============================================================
# ENDPOINTS PROTEGIDOS
# =============================================================
@app.post("/procesar/sav", dependencies=[Depends(verificar_token)])
async def procesar_sav(file: UploadFile = File(...)):
    from app.services.sav_av import procesar_sav_av
    contenido = await file.read()
    nombre = file.filename
    job_id = _create_job()
    t0 = time.time()
    def run():
        try:
            resultado = procesar_sav_av(contenido, nombre, "SAV", get_output_dir("SAV"),
                                        progress_cb=lambda s: _emit(job_id, s, time.time()-t0))
            resultado["archivos"] = _archivos_generados(resultado)
            _copiar_archivos_procesados_a_local(resultado, "SAV")
            _copiar_archivo_base(contenido, nombre, "SAV")
            _emit(job_id, "Completado", time.time()-t0, done=True, result=resultado)
        except Exception as e:
            _emit(job_id, str(e), time.time()-t0, done=True, error=str(e))
    _executor.submit(run)
    return {"job_id": job_id}

@app.post("/procesar/av", dependencies=[Depends(verificar_token)])
async def procesar_av(file: UploadFile = File(...)):
    from app.services.sav_av import procesar_sav_av
    contenido = await file.read()
    nombre = file.filename
    job_id = _create_job()
    t0 = time.time()
    def run():
        try:
            resultado = procesar_sav_av(contenido, nombre, "AV", get_output_dir("AV"),
                                        progress_cb=lambda s: _emit(job_id, s, time.time()-t0))
            resultado["archivos"] = _archivos_generados(resultado)
            _copiar_archivos_procesados_a_local(resultado, "AV")
            _copiar_archivo_base(contenido, nombre, "AV")
            _emit(job_id, "Completado", time.time()-t0, done=True, result=resultado)
        except Exception as e:
            _emit(job_id, str(e), time.time()-t0, done=True, error=str(e))
    _executor.submit(run)
    return {"job_id": job_id}

@app.post("/procesar/refi", dependencies=[Depends(verificar_token)])
async def procesar_refi(user: dict = Depends(verificar_token)):
    from app.services.refi_pl import procesar_refi_pl
    usuario = user.get("usuario", "")
    job_id = _create_job()
    t0 = time.time()
    def run():
        try:
            resultado = procesar_refi_pl(tipo="REFI", output_dir=get_output_dir("REFI"),
                                         progress_cb=lambda s: _emit(job_id, s, time.time()-t0),
                                         usuario=usuario)
            archivo_bytes = resultado.pop("_archivo_bytes", None)
            nombre_archivo = resultado.pop("_nombre_archivo", None)
            resultado["archivos"] = _archivos_generados(resultado)
            _copiar_archivos_procesados_a_local(resultado, "REFI")
            if archivo_bytes and nombre_archivo:
                _copiar_archivo_base(archivo_bytes, nombre_archivo, "REFI")
            _emit(job_id, "Completado", time.time()-t0, done=True, result=resultado)
        except Exception as e:
            _emit(job_id, str(e), time.time()-t0, done=True, error=str(e))
    _executor.submit(run)
    return {"job_id": job_id}

@app.post("/procesar/pl", dependencies=[Depends(verificar_token)])
async def procesar_pl(user: dict = Depends(verificar_token)):
    from app.services.refi_pl import procesar_refi_pl
    usuario = user.get("usuario", "")
    job_id = _create_job()
    t0 = time.time()
    def run():
        try:
            resultado = procesar_refi_pl(tipo="PL", output_dir=get_output_dir("PL"),
                                         progress_cb=lambda s: _emit(job_id, s, time.time()-t0),
                                         usuario=usuario)
            archivo_bytes = resultado.pop("_archivo_bytes", None)
            nombre_archivo = resultado.pop("_nombre_archivo", None)
            resultado["archivos"] = _archivos_generados(resultado)
            _copiar_archivos_procesados_a_local(resultado, "PL")
            if archivo_bytes and nombre_archivo:
                _copiar_archivo_base(archivo_bytes, nombre_archivo, "PL")
            _emit(job_id, "Completado", time.time()-t0, done=True, result=resultado)
        except Exception as e:
            _emit(job_id, str(e), time.time()-t0, done=True, error=str(e))
    _executor.submit(run)
    return {"job_id": job_id}

@app.post("/procesar/perdidas", dependencies=[Depends(verificar_token)])
async def procesar_perdidas(file: UploadFile = File(...)):
    from app.services.perdidas import procesar_llamadas_perdidas
    contenido = await file.read()
    nombre = file.filename
    job_id = _create_job()
    t0 = time.time()
    def run():
        try:
            # PERDIDAS usa carpeta con día igual que los demás
            output_dir = get_output_dir("PERDIDAS")
            resultado = procesar_llamadas_perdidas(contenido, nombre, output_dir,
                                                   progress_cb=lambda s: _emit(job_id, s, time.time()-t0))
            resultado["archivos"] = _archivos_generados(resultado)
            _copiar_archivos_procesados_a_local(resultado, "PERDIDAS")
            _copiar_archivo_base(contenido, nombre, "PERDIDAS")
            _emit(job_id, "Completado", time.time()-t0, done=True, result=resultado)
        except Exception as e:
            _emit(job_id, str(e), time.time()-t0, done=True, error=str(e))
    _executor.submit(run)
    return {"job_id": job_id}

@app.post("/lista-negra/actualizar", dependencies=[Depends(verificar_token)])
async def actualizar_lista_negra(file: UploadFile = File(None)):
    try:
        from app.services.lista_negra import procesar_lista_negra
        if file:
            contenido = await file.read()
            return procesar_lista_negra(archivo_bytes=contenido, nombre_archivo=file.filename)
        return procesar_lista_negra()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/lista-negra/total", dependencies=[Depends(verificar_token)])
async def get_total_lista_negra():
    from app.core.postgres import get_total_lista_negra
    return get_total_lista_negra()

@app.get("/logs", dependencies=[Depends(verificar_token)])
async def get_logs(limit: int = 50):
    from app.core.postgres import get_logs
    return get_logs(limit=limit)

@app.get("/config/general", dependencies=[Depends(verificar_token)])
async def get_config_general():
    cfg = _leer_config()
    return {
        "guardar_local":      cfg.get("guardar_local", False),
        "guardar_compartida": cfg.get("guardar_compartida", True),
    }

@app.put("/config/general", dependencies=[Depends(verificar_token)])
async def set_config_general(body: dict, user: dict = Depends(verificar_token)):
    cfg_antes = _leer_config()
    datos = {}
    if "guardar_local" in body:
        datos["guardar_local"] = bool(body["guardar_local"])
    if "guardar_compartida" in body:
        datos["guardar_compartida"] = bool(body["guardar_compartida"])
    _guardar_config(datos)
    cambios = []
    if datos.get("guardar_local") != cfg_antes.get("guardar_local"):
        cambios.append(f"guardar_local: {cfg_antes.get('guardar_local')} → {datos.get('guardar_local')}")
    if "guardar_compartida" in datos and datos.get("guardar_compartida") != cfg_antes.get("guardar_compartida"):
        cambios.append(f"guardar_compartida: {cfg_antes.get('guardar_compartida')} → {datos.get('guardar_compartida')}")
    if cambios:
        registrar_auditoria(
            usuario=user.get("usuario", ""),
            accion="Config general actualizada",
            detalle=", ".join(cambios),
        )
    return await get_config_general()

@app.get("/config/rutas", dependencies=[Depends(verificar_token)])
async def get_rutas():
    cfg = _leer_config()
    result = {}
    for tipo in RUTAS_COMPARTIDA_DEFAULT:
        key_c = f"ruta_{tipo.lower()}_compartida"
        key_l = f"ruta_{tipo.lower()}_local"
        result[key_c] = cfg.get(key_c, RUTAS_COMPARTIDA_DEFAULT[tipo])
        result[key_l] = cfg.get(key_l, RUTAS_LOCAL_DEFAULT[tipo])
    return result

@app.put("/config/rutas", dependencies=[Depends(verificar_token)])
async def set_rutas(body: dict, user: dict = Depends(verificar_token)):
    cfg_antes = _leer_config()
    datos = {}
    for tipo in RUTAS_COMPARTIDA_DEFAULT:
        for variante in ("compartida", "local"):
            key = f"ruta_{tipo.lower()}_{variante}"
            if key in body:
                ruta = body[key].strip()
                datos[key] = ruta
                if ruta:
                    try:
                        os.makedirs(ruta, exist_ok=True)
                    except Exception as e:
                        print(f"No se pudo crear {ruta}: {e}")
    _guardar_config(datos)
    cambios = {k: v for k, v in datos.items() if cfg_antes.get(k) != v}
    if cambios:
        registrar_auditoria(
            usuario=user.get("usuario", ""),
            accion="Rutas actualizadas",
            detalle=", ".join(f"{k}: {cfg_antes.get(k)} → {v}" for k, v in cambios.items())
        )
    return await get_rutas()

@app.get("/config/iddatabase", dependencies=[Depends(verificar_token)])
async def get_iddatabase():
    cfg = _leer_config()
    return {
        "IDDATABASE_SAV":  cfg.get("IDDATABASE_SAV",  218),
        "IDDATABASE_AV":   cfg.get("IDDATABASE_AV",   92),
        "IDDATABASE_PL":   cfg.get("IDDATABASE_PL",   131),
        "IDDATABASE_REFI": cfg.get("IDDATABASE_REFI", 70),
    }

@app.put("/config/iddatabase", dependencies=[Depends(verificar_token)])
async def set_iddatabase(body: dict, user: dict = Depends(verificar_token)):
    cfg_antes = _leer_config()
    campos = ["IDDATABASE_SAV", "IDDATABASE_AV", "IDDATABASE_PL", "IDDATABASE_REFI"]
    datos = {c: int(body[c]) for c in campos if c in body}
    _guardar_config(datos)
    cambios = {k: v for k, v in datos.items() if cfg_antes.get(k) != v}
    if cambios:
        registrar_auditoria(
            usuario=user.get("usuario", ""),
            accion="IDs de BD actualizados",
            detalle=", ".join(f"{k}: {cfg_antes.get(k)} → {v}" for k, v in cambios.items())
        )
    return _leer_config()

@app.get("/auditoria", dependencies=[Depends(verificar_token)])
async def get_auditoria_endpoint(limit: int = 100):
    return get_auditoria(limit=limit)

@app.get("/descargar", dependencies=[Depends(verificar_token)])
async def descargar_archivo(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(path, filename=os.path.basename(path))

# /health es publico para el ping del frontend
@app.get("/health")
async def health():
    cfg = _leer_config()
    return {
        "status": "ok",
        "guardar_local": cfg.get("guardar_local", False),
        "rutas": {
            t: {
                "compartida": cfg.get(f"ruta_{t.lower()}_compartida", RUTAS_COMPARTIDA_DEFAULT[t]),
                "local":      cfg.get(f"ruta_{t.lower()}_local",      RUTAS_LOCAL_DEFAULT[t]),
            }
            for t in RUTAS_COMPARTIDA_DEFAULT
        }
    }