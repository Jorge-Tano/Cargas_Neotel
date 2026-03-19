'use client'

import { useEffect, useState } from 'react'
import { Save, ChevronDown, CheckCircle2, Database, Network, HardDrive, FolderOpen, ChevronRight, Server } from 'lucide-react'
import { CASOS, CasoKey, API } from '../lib/api'
import { getToken } from '../hooks/useAuth'

function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const token = getToken()
  return { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...extra }
}

// ── Tipos ────────────────────────────────────────────────────

interface IddatabaseConfig {
  IDDATABASE_SAV:  number
  IDDATABASE_AV:   number
  IDDATABASE_PL:   number
  IDDATABASE_REFI: number
  DB_SAV_AV: string
  DB_AV:     string
  DB_PL:     string
  DB_REFI:   string
}

const IDS_VACIOS: IddatabaseConfig = {
  IDDATABASE_SAV: 0, IDDATABASE_AV: 0, IDDATABASE_PL: 0, IDDATABASE_REFI: 0,
  DB_SAV_AV: '', DB_AV: '', DB_PL: '', DB_REFI: '',
}

// Credenciales (host, puerto, usuario, contraseña) vienen del .env — no se muestran en la UI.
// Path base y paths por caso SÍ son configurables desde aquí (cambian por año/operación).
const SFTP_VACIO: Record<string, string> = {
  sftp_keyword_global: '',
  sftp_keyword_SAV: '', sftp_keyword_AV: '', sftp_keyword_REFI: '', sftp_keyword_PL: '',
}

// ── Configuración estática de casos (sin defaults de producción) ──

const ID_LABELS: { key: keyof IddatabaseConfig; caso: CasoKey; label: string; keyDb: keyof IddatabaseConfig }[] = [
  { key: 'IDDATABASE_SAV',  caso: 'SAV',  label: 'SAV',         keyDb: 'DB_SAV_AV' },
  { key: 'IDDATABASE_AV',   caso: 'AV',   label: 'AV',          keyDb: 'DB_AV'     },
  { key: 'IDDATABASE_PL',   caso: 'PL',   label: 'Pago Liviano', keyDb: 'DB_PL'    },
  { key: 'IDDATABASE_REFI', caso: 'REFI', label: 'REFI',        keyDb: 'DB_REFI'   },
]

const CASOS_CONFIG: { key: string; label: string; color: string }[] = [
  { key: 'SAV',      label: 'SAV Leakage',       color: CASOS.SAV.color      },
  { key: 'AV',       label: 'Avance Leakage',    color: CASOS.AV.color       },
  { key: 'REFI',     label: 'REFI Leakage',      color: CASOS.REFI.color     },
  { key: 'PL',       label: 'Pago Liviano',      color: CASOS.PL.color       },
  { key: 'PERDIDAS', label: 'Llamadas Perdidas', color: '#64748b'             },
]

// ── Helpers ──────────────────────────────────────────────────

const rutaCorta = (ruta: string) => {
  const partes = ruta.split('\\').filter(Boolean)
  return partes.length > 2 ? `...\\${partes.slice(-2).join('\\')}` : ruta
}

const Toggle = ({
  active, disabled, onChange,
}: { active: boolean; disabled: boolean; onChange: () => void }) => (
  <button
    onClick={onChange}
    disabled={disabled}
    className={`relative w-10 h-5 rounded-full transition-colors duration-200 focus:outline-none disabled:opacity-60 flex-shrink-0 ${active ? 'bg-blue-500' : 'bg-slate-200'}`}
  >
    <span className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform duration-200 ${active ? 'translate-x-5' : ''}`} />
  </button>
)

const SavedBadge = ({ visible }: { visible: boolean }) =>
  visible ? (
    <span className="flex items-center gap-1 text-xs text-emerald-600 animate-fade-in">
      <CheckCircle2 size={11} /> Guardado
    </span>
  ) : null

const SaveBtn = ({
  onClick, loading, label, color = '#0c90e6',
}: { onClick: () => void; loading: boolean; label: string; color?: string }) => (
  <button
    onClick={onClick}
    disabled={loading}
    className="flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-medium text-white disabled:opacity-60"
    style={{ backgroundColor: color }}
  >
    {loading ? <span className="spinner" /> : <Save size={11} />}
    {label}
  </button>
)

// ── Componente principal ──────────────────────────────────────

export function ConfigPanel() {
  // Panel abierto — solo uno a la vez ('toggles' | 'ids' | 'rutas' | 'sftp' | null)
  const [panelAbierto, setPanelAbierto] = useState<string | null>(null)

  const abrirPanel = (panel: string) =>
    setPanelAbierto(prev => { setAbierto(null); return prev === panel ? null : panel })

  // Toggles
  const [guardarLocal,      setGuardarLocal]      = useState(false)
  const [guardarCompartida, setGuardarCompartida] = useState(true)
  const [guardandoToggle,   setGuardandoToggle]   = useState(false)
  const [guardadoToggle,    setGuardadoToggle]    = useState(false)

  // Rutas locales por usuario
  const [mostrarRutasLocal, setMostrarRutasLocal] = useState(false)
  const [rutasLocal,        setRutasLocal]        = useState<Record<string, string>>({})
  const [guardandoLocal,    setGuardandoLocal]    = useState(false)
  const [guardadoLocal,     setGuardadoLocal]     = useState(false)

  // Acordeón interno de rutas y sftp
  const [abierto,       setAbierto]       = useState<string | null>(null)
  const [rutas,         setRutas]         = useState<Record<string, string>>({})
  const [rutasEdit,     setRutasEdit]     = useState<Record<string, string>>({})
  const [guardandoRuta, setGuardandoRuta] = useState<string | null>(null)
  const [guardadoRuta,  setGuardadoRuta]  = useState<string | null>(null)

  // IDs de BD
  const [ids,          setIds]          = useState<IddatabaseConfig>(IDS_VACIOS)
  const [idsOrig,      setIdsOrig]      = useState<IddatabaseConfig>(IDS_VACIOS)
  const [guardandoIds, setGuardandoIds] = useState(false)
  const [guardadoIds,  setGuardadoIds]  = useState(false)

  // SFTP
  const [sftp,          setSftp]          = useState<Record<string, string>>(SFTP_VACIO)
  const [sftpOrig,      setSftpOrig]      = useState<Record<string, string>>(SFTP_VACIO)
  const [guardandoSftp, setGuardandoSftp] = useState(false)
  const [guardadoSftp,  setGuardadoSftp]  = useState(false)

  // Fechas para preview de rutas SFTP
  const hoy = new Date()

  // ── Carga inicial ────────────────────────────────────────────

  useEffect(() => {
    fetch(`${API}/config/general`, { headers: authHeaders() })
      .then(r => r.json())
      .then(d => {
        setGuardarLocal(d.guardar_local ?? false)
        setGuardarCompartida(d.guardar_compartida ?? true)
      })

    fetch(`${API}/config/rutas`, { headers: authHeaders() })
      .then(r => r.json())
      .then(d => {
        setRutas(d)
        setRutasEdit(d)
        const locales: Record<string, string> = {}
        CASOS_CONFIG.forEach(c => {
          locales[c.key] = d[`ruta_${c.key.toLowerCase()}_local`] ?? ''
        })
        setRutasLocal(locales)
      })

    fetch(`${API}/config/iddatabase`, { headers: authHeaders() })
      .then(r => r.json())
      .then(d => { setIds(d); setIdsOrig(d) })

    fetch(`${API}/config/sftp`, { headers: authHeaders() })
      .then(r => r.json())
      .then(d => {
        const merged = { ...SFTP_VACIO, ...d }
        setSftp(merged)
        setSftpOrig(merged)
      })

    return () => { setAbierto(null); setPanelAbierto(null) }
  }, [])

  // ── Acciones ─────────────────────────────────────────────────

  const toggleConfig = async (nuevoLocal: boolean, nuevaCompartida: boolean) => {
    setGuardarLocal(nuevoLocal)
    setGuardarCompartida(nuevaCompartida)
    setGuardandoToggle(true)
    try {
      await fetch(`${API}/config/general`, {
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ guardar_local: nuevoLocal, guardar_compartida: nuevaCompartida }),
      })
      setGuardadoToggle(true)
      setTimeout(() => setGuardadoToggle(false), 2000)
    } finally { setGuardandoToggle(false) }
  }

  const guardarRutasLocal = async () => {
    setGuardandoLocal(true)
    try {
      const body: Record<string, string> = {}
      CASOS_CONFIG.forEach(c => { body[`ruta_${c.key.toLowerCase()}_local`] = rutasLocal[c.key] ?? '' })
      const res = await fetch(`${API}/config/rutas`, {
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(body),
      })
      setRutas(await res.json())
      setGuardadoLocal(true)
      setTimeout(() => setGuardadoLocal(false), 2500)
    } finally { setGuardandoLocal(false) }
  }

  const getEditKey = (key: string, variante: 'compartida' | 'local') =>
    `ruta_${key.toLowerCase()}_${variante}`

  const getRutaActual = (key: string, variante: 'compartida' | 'local') =>
    rutas[getEditKey(key, variante)] ?? ''

  const abrirCaso = (key: string) => {
    if (abierto === key) { setAbierto(null); return }
    setRutasEdit(prev => ({ ...prev, [getEditKey(key, 'compartida')]: getRutaActual(key, 'compartida') }))
    setAbierto(key)
  }

  const guardarRutaCaso = async (key: string) => {
    setGuardandoRuta(key)
    try {
      const body = { [getEditKey(key, 'compartida')]: rutasEdit[getEditKey(key, 'compartida')] ?? '' }
      const res = await fetch(`${API}/config/rutas`, {
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(body),
      })
      setRutas(await res.json())
      setAbierto(null)
      setGuardadoRuta(key)
      setTimeout(() => setGuardadoRuta(null), 2500)
    } finally { setGuardandoRuta(null) }
  }

  const guardarIds = async () => {
    setGuardandoIds(true)
    try {
      await fetch(`${API}/config/iddatabase`, {
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(ids),
      })
      setIdsOrig({ ...ids })
      setGuardadoIds(true)
      setTimeout(() => setGuardadoIds(false), 2500)
    } finally { setGuardandoIds(false) }
  }

  const guardarSftp = async () => {
    setGuardandoSftp(true)
    try {
      const payload = {
        sftp_keyword_global: sftp.sftp_keyword_global,
        sftp_keyword_SAV:    sftp.sftp_keyword_SAV,
        sftp_keyword_AV:     sftp.sftp_keyword_AV,
        sftp_keyword_REFI:   sftp.sftp_keyword_REFI,
        sftp_keyword_PL:     sftp.sftp_keyword_PL,
      }
      console.log('[SFTP] Enviando:', payload)
      const res = await fetch(`${API}/config/sftp`, {
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(payload),
      })
      const data = await res.json()
      console.log('[SFTP] Respuesta:', data)
      setSftp(data)
      setSftpOrig(data)
      setGuardadoSftp(true)
      setTimeout(() => setGuardadoSftp(false), 2500)
    } finally { setGuardandoSftp(false) }
  }

  // ── Render ───────────────────────────────────────────────────

  return (
    <div className="animate-fade-in space-y-3">

      {/* Fila 1: Toggles + IDs BD */}
      <div className="grid grid-cols-2 gap-3 items-start">

        {/* Toggles guardar */}
        <div className="rounded-2xl border border-slate-200 bg-white shadow-sm px-4 py-3 space-y-2">
          <div className="grid grid-cols-2 gap-2">
            {/* Toggle: Red */}
            <div className="flex items-center justify-between rounded-xl border border-slate-100 bg-slate-50 px-3 py-2">
              <div className="flex items-center gap-2">
                <div className="w-6 h-6 rounded-lg bg-white flex items-center justify-center flex-shrink-0 shadow-sm">
                  <Network size={12} className="text-slate-500" />
                </div>
                <div>
                  <p className="text-xs font-semibold text-slate-800 leading-tight">Red</p>
                  <p className="text-xs text-slate-400">{guardarCompartida ? 'Activo' : 'Off'}</p>
                </div>
              </div>
              <div className="flex items-center gap-1">
                {guardadoToggle && <CheckCircle2 size={11} className="text-emerald-500" />}
                <Toggle active={guardarCompartida} disabled={guardandoToggle}
                  onChange={() => toggleConfig(guardarLocal, !guardarCompartida)} />
              </div>
            </div>
            {/* Toggle: Local */}
            <div className="flex items-center justify-between rounded-xl border border-slate-100 bg-slate-50 px-3 py-2">
              <div className="flex items-center gap-2">
                <div className="w-6 h-6 rounded-lg bg-white flex items-center justify-center flex-shrink-0 shadow-sm">
                  <HardDrive size={12} className="text-slate-500" />
                </div>
                <div>
                  <p className="text-xs font-semibold text-slate-800 leading-tight">Local</p>
                  <p className="text-xs text-slate-400">{guardarLocal ? 'Activo' : 'Off'}</p>
                </div>
              </div>
              <div className="flex items-center gap-1">
                <Toggle active={guardarLocal} disabled={guardandoToggle}
                  onChange={() => toggleConfig(!guardarLocal, guardarCompartida)} />
                {guardarLocal && (
                  <button onClick={() => setMostrarRutasLocal(!mostrarRutasLocal)} className="text-blue-500 hover:text-blue-700">
                    <ChevronRight size={12} className={`transition-transform ${mostrarRutasLocal ? 'rotate-90' : ''}`} />
                  </button>
                )}
              </div>
            </div>
          </div>

          {/* Rutas locales expandibles */}
          {guardarLocal && mostrarRutasLocal && (
            <div className="pt-1 border-t border-slate-100 space-y-1.5 animate-fade-in">
              <p className="text-xs text-slate-400">Carpeta exacta donde se guardarán los archivos.</p>
              <div className="grid grid-cols-2 gap-x-3 gap-y-1">
                {CASOS_CONFIG.map(caso => (
                  <div key={caso.key}>
                    <div className="flex items-center gap-1 mb-0.5">
                      <div className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: caso.color }} />
                      <span className="text-xs text-slate-500 truncate">{caso.label}</span>
                    </div>
                    <input type="text" value={rutasLocal[caso.key] ?? ''}
                      onChange={e => setRutasLocal(p => ({ ...p, [caso.key]: e.target.value }))}
                      className="w-full rounded-lg border border-slate-200 bg-slate-50 px-2 py-1 text-xs font-mono text-slate-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
                      placeholder="C:\Cargas"
                    />
                  </div>
                ))}
              </div>
              <div className="flex items-center gap-2 pt-0.5">
                <SaveBtn onClick={guardarRutasLocal} loading={guardandoLocal} label="Guardar" />
                <SavedBadge visible={guardadoLocal} />
              </div>
            </div>
          )}
        </div>

        {/* IDs BD */}
        <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
          <button onClick={() => abrirPanel("ids")}
            className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50 transition-colors">
            <div className="flex items-center gap-2">
              <div className="w-6 h-6 rounded-lg bg-blue-50 flex items-center justify-center flex-shrink-0">
                <Database size={13} className="text-blue-500" />
              </div>
              <div className="text-left">
                <p className="text-xs font-semibold text-slate-800 leading-tight">IDs de base de datos</p>
                <p className="text-xs text-slate-400">Actualizar mensualmente</p>
              </div>
            </div>
            <ChevronRight size={13} className={`text-slate-400 transition-transform ${panelAbierto === "ids" ? "rotate-90" : ""}`} />
          </button>

          {panelAbierto === "ids" && (
            <div className="border-t border-slate-100 px-3 py-2.5 space-y-2 animate-fade-in">
              <div className="grid grid-cols-2 gap-x-3 gap-y-2">
                {ID_LABELS.map(({ key, caso, label, keyDb }) => {
                  const cambio = ids[key] !== idsOrig[key] || ids[keyDb] !== idsOrig[keyDb]
                  const cls = `rounded-lg border px-2 py-1 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-blue-200 transition-colors ${cambio ? 'border-amber-300 bg-amber-50' : 'border-slate-200'}`
                  return (
                    <div key={key} className="space-y-1">
                      <div className="flex items-center gap-1.5">
                        <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: CASOS[caso].color }} />
                        <span className="text-xs font-medium text-slate-600">{label}</span>
                      </div>
                      <div className="flex gap-1.5">
                        <input type="number" value={ids[key] as number} placeholder="ID"
                          onChange={e => setIds(p => ({ ...p, [key]: parseInt(e.target.value) || 0 }))}
                          className={`${cls} w-16 text-center flex-shrink-0`} />
                        <input type="text" value={ids[keyDb] as string} placeholder="ECRM_0000"
                          onChange={e => setIds(p => ({ ...p, [keyDb]: e.target.value }))}
                          className={`${cls} flex-1 min-w-0`} />
                      </div>
                    </div>
                  )
                })}
              </div>
              <div className="flex items-center gap-2 pt-1 border-t border-slate-100">
                <SaveBtn onClick={guardarIds} loading={guardandoIds} label="Guardar IDs" />
                <SavedBadge visible={guardadoIds} />
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Fila 2: Rutas compartidas + SFTP */}
      <div className="grid grid-cols-2 gap-3 items-start">

        {/* Rutas compartidas por proceso */}
        <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
          <button onClick={() => abrirPanel("rutas")}
            className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50 transition-colors">
            <div className="flex items-center gap-2">
              <div className="w-6 h-6 rounded-lg bg-slate-100 flex items-center justify-center flex-shrink-0">
                <FolderOpen size={13} className="text-slate-500" />
              </div>
              <div className="text-left">
                <p className="text-xs font-semibold text-slate-800 leading-tight">Rutas de destino por proceso</p>
                <p className="text-xs text-slate-400">Carpeta compartida de cada caso</p>
              </div>
            </div>
            <ChevronRight size={13} className={`text-slate-400 transition-transform ${panelAbierto === "rutas" ? "rotate-90" : ""}`} />
          </button>

          {panelAbierto === "rutas" && (
            <div className="border-t border-slate-100 px-3 py-2 animate-fade-in space-y-2">
              <div className="grid grid-cols-2 gap-1.5">
                {CASOS_CONFIG.map(caso => {
                  const open      = abierto === caso.key
                  const guardando = guardandoRuta === caso.key
                  const guardado  = guardadoRuta  === caso.key
                  const rutaActual = getRutaActual(caso.key, 'compartida')

                  return (
                    <div key={caso.key}
                      className="rounded-xl border transition-all duration-200 overflow-hidden"
                      style={{ borderColor: open ? `${caso.color}50` : '#e2e8f0', backgroundColor: open ? `${caso.color}06` : 'white' }}
                    >
                      <button onClick={() => abrirCaso(caso.key)}
                        className="w-full flex items-center gap-2 px-2.5 py-2 text-left">
                        <div className="w-5 h-5 rounded-md flex items-center justify-center flex-shrink-0"
                          style={{ backgroundColor: `${caso.color}18` }}>
                          <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: caso.color }} />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-1">
                            <span className="text-xs font-semibold text-slate-800">{caso.label}</span>
                            {guardado && <CheckCircle2 size={9} className="text-emerald-500" />}
                          </div>
                          {!open && (
                            <p className={`text-xs font-mono truncate ${rutaActual ? 'text-slate-400' : 'text-amber-400'}`}>
                              {rutaActual ? rutaCorta(rutaActual) : 'Sin configurar'}
                            </p>
                          )}
                        </div>
                        <ChevronDown size={12} className={`text-slate-400 flex-shrink-0 transition-transform ${open ? 'rotate-180' : ''}`} />
                      </button>

                      {open && (
                        <div className="px-2.5 pb-2.5 space-y-1.5 border-t" style={{ borderColor: `${caso.color}20` }}>
                          <div className="pt-1.5">
                            <label className="flex items-center gap-1 text-xs font-semibold mb-1" style={{ color: caso.color }}>
                              <Network size={9} /> Red compartida
                            </label>
                            <input type="text"
                              value={rutasEdit[getEditKey(caso.key, 'compartida')] ?? ''}
                              onChange={e => setRutasEdit(p => ({ ...p, [getEditKey(caso.key, 'compartida')]: e.target.value }))}
                              className="w-full rounded-lg border px-2 py-1 text-xs font-mono text-slate-700 focus:outline-none transition-colors"
                              style={{ borderColor: `${caso.color}40`, backgroundColor: `${caso.color}06` }}
                              placeholder="\\servidor\ruta\..."
                            />
                          </div>
                          <div className="flex items-center gap-1.5">
                            <button onClick={() => guardarRutaCaso(caso.key)} disabled={!!guardando}
                              className="flex items-center gap-1 rounded-lg px-2 py-1 text-xs font-semibold text-white disabled:opacity-60"
                              style={{ backgroundColor: caso.color }}>
                              {guardando ? <span className="spinner" /> : <Save size={9} />}
                              Guardar
                            </button>
                            <button onClick={() => setAbierto(null)}
                              className="text-xs px-2 py-1 rounded-lg bg-slate-100 text-slate-500 hover:bg-slate-200">
                              Cancelar
                            </button>
                          </div>
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
              {/* Ruta Lista Negra */}
              <div className="border-t border-slate-100 pt-2">
                <div className="flex items-center gap-1 mb-1">
                  <div className="w-1.5 h-1.5 rounded-full bg-slate-400 flex-shrink-0" />
                  <span className="text-xs font-medium text-slate-600">Lista Negra</span>
                </div>
                <div className="flex gap-1.5">
                  <input type="text"
                    value={rutasEdit['ruta_blacklist_red'] ?? rutas['ruta_blacklist_red'] ?? ''}
                    onChange={e => setRutasEdit(p => ({ ...p, ruta_blacklist_red: e.target.value }))}
                    className="flex-1 rounded-lg border border-slate-200 px-2 py-1 text-xs font-mono text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-200 transition-colors"
                    placeholder="\\servidor\ruta\Bloqueos"
                  />
                  <button
                    onClick={async () => {
                      const res = await fetch(`${API}/config/rutas`, {
                        method: 'PUT',
                        headers: authHeaders({ 'Content-Type': 'application/json' }),
                        body: JSON.stringify({ ruta_blacklist_red: rutasEdit['ruta_blacklist_red'] ?? '' }),
                      })
                      setRutas(await res.json())
                    }}
                    className="flex items-center gap-1 rounded-lg px-2 py-1 text-xs font-semibold text-white bg-slate-500 hover:bg-slate-600"
                  >
                    <Save size={9} /> Guardar
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* SFTP — ruta y patrón por caso (credenciales vienen del .env) */}
        <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
          <button onClick={() => abrirPanel("sftp")}
            className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50 transition-colors">
            <div className="flex items-center gap-2">
              <div className="w-6 h-6 rounded-lg bg-emerald-50 flex items-center justify-center flex-shrink-0">
                <Server size={13} className="text-emerald-500" />
              </div>
              <div className="text-left">
                <p className="text-xs font-semibold text-slate-800 leading-tight">Rutas y patrones SFTP</p>
                <p className="text-xs text-slate-400">Ruta y palabras clave por proceso</p>
              </div>
            </div>
            <ChevronRight size={13} className={`text-slate-400 transition-transform ${panelAbierto === "sftp" ? "rotate-90" : ""}`} />
          </button>

          {panelAbierto === "sftp" && (
            <div className="border-t border-slate-100 px-3 py-2 space-y-2 animate-fade-in">

              {/* Global + por caso compacto */}
              <div className="flex items-center gap-2 mb-1">
                <span className="text-xs text-slate-400 flex-shrink-0 w-12">Global</span>
                <input type="text" value={sftp.sftp_keyword_global ?? ''}
                  onChange={e => setSftp(p => ({ ...p, sftp_keyword_global: e.target.value }))}
                  className={`flex-1 rounded-lg border px-2 py-1 text-xs font-mono focus:outline-none transition-colors ${sftp.sftp_keyword_global !== sftpOrig.sftp_keyword_global ? 'border-amber-300 bg-amber-50' : 'border-slate-200'}`}
                  placeholder="LEAKAGE"
                />
              </div>

              <div className="grid grid-cols-2 gap-1.5">
                {(['SAV', 'AV', 'REFI', 'PL'] as CasoKey[]).map(caso => {
                  const keyKw     = `sftp_keyword_${caso}`
                  const color     = CASOS[caso].color
                  const openSftp  = abierto === `sftp_${caso}`
                  const changed   = sftp[keyKw] !== sftpOrig[keyKw]
                  const hoy       = new Date()
                  const mes       = hoy.toLocaleString('es', { month: 'long' }).toUpperCase()
                  const kwGlobal  = sftp.sftp_keyword_global || 'LEAKAGE'
                  const kwFija    = sftp[keyKw] || caso

                  return (
                    <div key={caso} className="rounded-xl border overflow-hidden transition-all"
                      style={{ borderColor: openSftp ? `${color}50` : '#e2e8f0', backgroundColor: openSftp ? `${color}06` : 'white' }}>
                      <button onClick={() => setAbierto(openSftp ? null : `sftp_${caso}`)}
                        className="w-full flex items-center gap-2 px-2.5 py-1.5 text-left">
                        <div className="w-4 h-4 rounded flex items-center justify-center flex-shrink-0" style={{ backgroundColor: `${color}18` }}>
                          <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color }} />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-1">
                            <span className="text-xs font-semibold text-slate-700">{caso}</span>
                            {changed && <span className="w-1.5 h-1.5 rounded-full bg-amber-400" />}
                          </div>
                          {!openSftp && (
                            <div className="flex gap-1 flex-wrap">
                              <span className="text-xs font-mono bg-slate-100 text-slate-400 rounded px-1 leading-tight">{kwGlobal}</span>
                              <span className="text-xs font-mono rounded px-1 leading-tight" style={{ backgroundColor: `${color}15`, color }}>{kwFija}</span>
                            </div>
                          )}
                        </div>
                        <ChevronDown size={11} className={`text-slate-400 flex-shrink-0 transition-transform ${openSftp ? 'rotate-180' : ''}`} />
                      </button>

                      {openSftp && (
                        <div className="px-2.5 pb-2 space-y-1 border-t" style={{ borderColor: `${color}20` }}>
                          <p className="pt-1 text-xs text-slate-400 font-mono bg-slate-50 rounded px-2 py-1 border border-slate-200 truncate">
                            {caso === 'SAV'  ? `/${hoy.getFullYear()}/SAV/${mes}/LEAKAGE` :
                             caso === 'AV'   ? `/${hoy.getFullYear()}/AV/LEAKAGE/${mes}` :
                             `/${hoy.getFullYear()}/OP/leakage`}
                          </p>
                          <input type="text" value={sftp[keyKw] ?? ''} placeholder={caso}
                            onChange={e => setSftp(p => ({ ...p, [keyKw]: e.target.value }))}
                            className={`w-full rounded-lg border px-2 py-1 text-xs font-mono focus:outline-none transition-colors ${sftp[keyKw] !== sftpOrig[keyKw] ? 'border-amber-300 bg-amber-50' : 'border-slate-200'}`}
                            style={{ backgroundColor: sftp[keyKw] !== sftpOrig[keyKw] ? undefined : `${color}06`, borderColor: sftp[keyKw] !== sftpOrig[keyKw] ? undefined : `${color}40` }}
                          />
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>

              <div className="flex items-center gap-2 pt-1 border-t border-slate-100">
                <SaveBtn onClick={guardarSftp} loading={guardandoSftp} label="Guardar SFTP" color="#059669" />
                <SavedBadge visible={guardadoSftp} />
              </div>
            </div>
          )}
        </div>

      </div>
    </div>
  )
}