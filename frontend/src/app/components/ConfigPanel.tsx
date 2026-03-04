'use client'

import { useEffect, useState } from 'react'
import { Save, ChevronDown, CheckCircle2, Database, Network, HardDrive, FolderOpen, ChevronRight } from 'lucide-react'
import { CASOS, CasoKey, API } from '../lib/api'
import { getToken } from '../hooks/useAuth'

function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const token = getToken()
  return { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...extra }
}

interface IddatabaseConfig {
  IDDATABASE_SAV:  number
  IDDATABASE_AV:   number
  IDDATABASE_PL:   number
  IDDATABASE_REFI: number
}

const ID_LABELS: { key: keyof IddatabaseConfig; caso: CasoKey; label: string }[] = [
  { key: 'IDDATABASE_SAV',  caso: 'SAV',  label: 'SAV' },
  { key: 'IDDATABASE_AV',   caso: 'AV',   label: 'AV' },
  { key: 'IDDATABASE_PL',   caso: 'PL',   label: 'Pago Liviano' },
  { key: 'IDDATABASE_REFI', caso: 'REFI', label: 'REFI' },
]

const CASOS_CONFIG: { key: string; label: string; color: string; defaultCompartida: string; defaultLocal: string }[] = [
  { key: 'SAV',      label: 'SAV Leakage',       color: CASOS.SAV.color,  defaultCompartida: '\\\\10.0.1.40\\informatica\\Neotel\\Presto\\Leakage\\SAV Leakage',    defaultLocal: 'C:\\Cargas\\Leakage\\SAV Leakage' },
  { key: 'AV',       label: 'Avance Leakage',    color: CASOS.AV.color,   defaultCompartida: '\\\\10.0.1.40\\informatica\\Neotel\\Presto\\Leakage\\Avance Leakage', defaultLocal: 'C:\\Cargas\\Leakage\\Avance Leakage' },
  { key: 'REFI',     label: 'REFI Leakage',      color: CASOS.REFI.color, defaultCompartida: '\\\\10.0.1.40\\informatica\\Neotel\\Presto\\REFI LEAKAGE',            defaultLocal: 'C:\\Cargas\\REFI LEAKAGE' },
  { key: 'PL',       label: 'Pago Liviano',      color: CASOS.PL.color,   defaultCompartida: '\\\\10.0.1.40\\informatica\\Neotel\\Presto\\PL LEAKAGE\\CARGAS',      defaultLocal: 'C:\\Cargas\\PL LEAKAGE\\CARGAS' },
  { key: 'PERDIDAS', label: 'Llamadas Perdidas', color: '#64748b',         defaultCompartida: '\\\\10.0.1.40\\informatica\\Neotel\\Presto\\Seguimiento PPFF',         defaultLocal: 'C:\\Cargas\\Seguimiento PPFF' },
]

export function ConfigPanel() {
  const [guardarLocal, setGuardarLocal]       = useState(false)
  const [guardandoToggle, setGuardandoToggle] = useState(false)
  const [guardadoToggle, setGuardadoToggle]   = useState(false)
  const [rutasLocal, setRutasLocal]           = useState<Record<string, string>>({})
  const [guardandoLocal, setGuardandoLocal]   = useState(false)
  const [guardadoLocal, setGuardadoLocal]     = useState(false)

  const [mostrarRutasLocal, setMostrarRutasLocal] = useState(false)
  const [mostrarRutas, setMostrarRutas]           = useState(false)
  const [abierto, setAbierto]             = useState<string | null>(null)
  const [rutas, setRutas]                 = useState<Record<string, string>>({})
  const [rutasEdit, setRutasEdit]         = useState<Record<string, string>>({})
  const [guardandoRuta, setGuardandoRuta] = useState<string | null>(null)
  const [guardadoRuta, setGuardadoRuta]   = useState<string | null>(null)

  const [mostrarIds, setMostrarIds]     = useState(false)
  const [ids, setIds]                   = useState<IddatabaseConfig>({ IDDATABASE_SAV: 217, IDDATABASE_AV: 91, IDDATABASE_PL: 135, IDDATABASE_REFI: 76 })
  const [idsOrig, setIdsOrig]           = useState<IddatabaseConfig>({ IDDATABASE_SAV: 217, IDDATABASE_AV: 91, IDDATABASE_PL: 135, IDDATABASE_REFI: 76 })
  const [guardandoIds, setGuardandoIds] = useState(false)
  const [guardadoIds, setGuardadoIds]   = useState(false)

  const hoy = new Date()
  const MESES_LABEL: Record<number, string> = {
    1:'01-Enero',2:'02-Febrero',3:'03-Marzo',4:'04-Abril',
    5:'05-Mayo',6:'06-Junio',7:'07-Julio',8:'08-Agosto',
    9:'09-Septiembre',10:'10-Octubre',11:'11-Noviembre',12:'12-Diciembre'
  }
  const mesStr  = MESES_LABEL[hoy.getMonth() + 1]
  const diaStr  = String(hoy.getDate()).padStart(2, '0')
  const anioStr = String(hoy.getFullYear())

  // Colapsar rutas locales al salir de la pestaña
  useEffect(() => {
    return () => {
      setAbierto(null)
      setMostrarRutas(false)
      setMostrarIds(false)
    }
  }, [])

  useEffect(() => {
    fetch(`${API}/config/general`, { headers: authHeaders() }).then(r => r.json()).then(d => setGuardarLocal(d.guardar_local))
    fetch(`${API}/config/rutas`, { headers: authHeaders() }).then(r => r.json()).then(d => {
      setRutas(d)
      setRutasEdit(d)
      const locales: Record<string, string> = {}
      CASOS_CONFIG.forEach(c => {
        locales[c.key] = d[`ruta_${c.key.toLowerCase()}_local`] ?? c.defaultLocal
      })
      setRutasLocal(locales)
    })
    fetch(`${API}/config/iddatabase`, { headers: authHeaders() }).then(r => r.json()).then(d => { setIds(d); setIdsOrig(d) })
  }, [])

  const toggleGuardarLocal = async (valor: boolean) => {
    setGuardarLocal(valor)
    setGuardandoToggle(true)
    try {
      await fetch(`${API}/config/general`, {
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ guardar_local: valor }),
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
        method: 'PUT', headers: authHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(body),
      })
      setRutas(await res.json())
      setGuardadoLocal(true)
      setTimeout(() => setGuardadoLocal(false), 2500)
    } finally { setGuardandoLocal(false) }
  }

  const getRutaActual = (key: string, variante: 'compartida' | 'local') => {
    const cfgKey = `ruta_${key.toLowerCase()}_${variante}`
    const caso = CASOS_CONFIG.find(c => c.key === key)
    return rutas[cfgKey] || (variante === 'compartida' ? caso?.defaultCompartida : caso?.defaultLocal) || ''
  }

  const getEditKey = (key: string, variante: 'compartida' | 'local') => `ruta_${key.toLowerCase()}_${variante}`

  const abrirCaso = (key: string) => {
    if (abierto === key) { setAbierto(null); return }
    setRutasEdit(prev => ({
      ...prev,
      [getEditKey(key, 'compartida')]: getRutaActual(key, 'compartida'),
    }))
    setAbierto(key)
  }

  const guardarRutaCaso = async (key: string) => {
    setGuardandoRuta(key)
    try {
      const body = {
        [getEditKey(key, 'compartida')]: rutasEdit[getEditKey(key, 'compartida')] ?? getRutaActual(key, 'compartida'),
      }
      const res = await fetch(`${API}/config/rutas`, {
        method: 'PUT', headers: authHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(body),
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
        method: 'PUT', headers: authHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(ids),
      })
      setIdsOrig({ ...ids })
      setGuardadoIds(true)
      setTimeout(() => setGuardadoIds(false), 2500)
    } finally { setGuardandoIds(false) }
  }

  const rutaCorta = (ruta: string) => {
    const partes = ruta.split('\\').filter(Boolean)
    return partes.length > 2 ? `...\\${partes.slice(-2).join('\\')}` : ruta
  }

  return (
    <div className="animate-fade-in space-y-3">

      {/* ── Fila superior: Toggle (col izq) + IDs BD (col der) ── */}
      <div className="grid grid-cols-2 gap-3 items-start">

        {/* Toggle guardar local */}
        <div className="rounded-2xl border border-slate-200 bg-white shadow-sm px-4 py-3 h-full">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2.5">
              <div className="w-7 h-7 rounded-lg bg-slate-100 flex items-center justify-center flex-shrink-0">
                <HardDrive size={13} className="text-slate-500" />
              </div>
              <div>
                <h3 className="font-semibold text-slate-800 text-sm">Guardar en local</h3>
                <p className="text-xs text-slate-400 leading-tight">
                  {guardarLocal ? 'Red y equipo local' : 'Solo carpeta de red'}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2 flex-shrink-0">
              {guardadoToggle && <CheckCircle2 size={12} className="text-emerald-500" />}
              <button
                onClick={() => toggleGuardarLocal(!guardarLocal)}
                disabled={guardandoToggle}
                className={`relative w-10 h-5 rounded-full transition-colors duration-200 focus:outline-none disabled:opacity-60 flex-shrink-0 ${guardarLocal ? 'bg-blue-500' : 'bg-slate-200'}`}
              >
                <span className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform duration-200 ${guardarLocal ? 'translate-x-5' : 'translate-x-0'}`} />
              </button>
              {guardarLocal && (
                <button
                  onClick={() => setMostrarRutasLocal(!mostrarRutasLocal)}
                  className="flex items-center gap-0.5 text-xs text-blue-500 hover:text-blue-700 transition-colors"
                >
                  <ChevronRight size={13} className={`transition-transform ${mostrarRutasLocal ? 'rotate-90' : ''}`} />
                </button>
              )}
            </div>
          </div>

          {guardarLocal && mostrarRutasLocal && (
            <div className="mt-3 pt-3 border-t border-slate-100 space-y-2 animate-fade-in">
              {CASOS_CONFIG.map(caso => (
                <div key={caso.key} className="flex items-center gap-2">
                  <div className="flex items-center gap-1.5 w-28 flex-shrink-0">
                    <div className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: caso.color }} />
                    <span className="text-xs text-slate-500 truncate">{caso.label}</span>
                  </div>
                  <input
                    type="text"
                    value={rutasLocal[caso.key] ?? ''}
                    onChange={e => setRutasLocal(prev => ({ ...prev, [caso.key]: e.target.value }))}
                    className="flex-1 rounded-lg border border-slate-200 bg-slate-50 px-2 py-1 text-xs font-mono text-slate-600 focus:outline-none focus:ring-2 focus:ring-blue-200 transition-colors"
                    placeholder="C:\Cargas\..."
                  />
                </div>
              ))}
              <div className="flex items-center gap-2 pt-0.5">
                <button
                  onClick={guardarRutasLocal}
                  disabled={guardandoLocal}
                  className="flex items-center gap-1 rounded-lg px-2.5 py-1 text-xs font-medium text-white disabled:opacity-60"
                  style={{ backgroundColor: '#0c90e6' }}
                >
                  {guardandoLocal ? <span className="spinner" /> : <Save size={10} />}
                  Guardar
                </button>
                {guardadoLocal && (
                  <span className="flex items-center gap-1 text-xs text-emerald-600 animate-fade-in">
                    <CheckCircle2 size={10} /> Guardado
                  </span>
                )}
              </div>
            </div>
          )}
        </div>

        {/* IDs BD */}
        <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
          <button
            onClick={() => setMostrarIds(!mostrarIds)}
            className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50 transition-colors"
          >
            <div className="flex items-center gap-2.5">
              <div className="w-7 h-7 rounded-lg bg-blue-50 flex items-center justify-center flex-shrink-0">
                <Database size={13} className="text-blue-500" />
              </div>
              <div className="text-left">
                <h3 className="font-semibold text-slate-800 text-sm">IDs de base de datos</h3>
                <p className="text-xs text-slate-400 leading-tight">Actualizar mensualmente</p>
              </div>
            </div>
            <ChevronRight size={14} className={`text-slate-400 transition-transform flex-shrink-0 ${mostrarIds ? 'rotate-90' : ''}`} />
          </button>

          {mostrarIds && (
            <div className="border-t border-slate-100 px-4 py-3 space-y-3 animate-fade-in">
              <div className="grid grid-cols-2 gap-x-3 gap-y-2">
                {ID_LABELS.map(({ key, caso, label }) => {
                  const cambio = ids[key] !== idsOrig[key]
                  return (
                    <div key={key} className="flex items-center gap-2">
                      <div className="flex items-center gap-1.5 flex-1 min-w-0">
                        <div className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: CASOS[caso].color }} />
                        <span className="text-xs font-medium text-slate-600 truncate">{label}</span>
                        <span className="text-xs text-slate-400 font-mono">({idsOrig[key]})</span>
                      </div>
                      <input
                        type="number"
                        value={ids[key]}
                        onChange={e => setIds(prev => ({ ...prev, [key]: parseInt(e.target.value) || 0 }))}
                        className={`w-16 rounded-lg border px-2 py-1 text-xs font-mono text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-200 text-center transition-colors ${cambio ? 'border-amber-300 bg-amber-50' : 'border-slate-200'}`}
                      />
                    </div>
                  )
                })}
              </div>
              <div className="flex items-center gap-2 pt-1 border-t border-slate-100">
                <button
                  onClick={guardarIds}
                  disabled={guardandoIds}
                  className="flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-medium text-white disabled:opacity-60"
                  style={{ backgroundColor: '#0c90e6' }}
                >
                  {guardandoIds ? <span className="spinner" /> : <Save size={11} />}
                  Guardar IDs
                </button>
                {guardadoIds && (
                  <span className="flex items-center gap-1 text-xs text-emerald-600 animate-fade-in">
                    <CheckCircle2 size={11} /> Guardado
                  </span>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ── Fila inferior: Rutas por proceso (ancho completo) ── */}
      <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
        <button
          onClick={() => setMostrarRutas(!mostrarRutas)}
          className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50 transition-colors"
        >
          <div className="flex items-center gap-2.5">
            <div className="w-7 h-7 rounded-lg bg-slate-100 flex items-center justify-center flex-shrink-0">
              <FolderOpen size={13} className="text-slate-500" />
            </div>
            <div className="text-left">
              <h3 className="font-semibold text-slate-800 text-sm">Rutas de destino por proceso</h3>
              <p className="text-xs text-slate-400 leading-tight">Ver y modificar dónde se guarda cada caso</p>
            </div>
          </div>
          <ChevronRight size={14} className={`text-slate-400 transition-transform flex-shrink-0 ${mostrarRutas ? 'rotate-90' : ''}`} />
        </button>

        {mostrarRutas && (
          <div className="border-t border-slate-100 px-3 py-2.5 animate-fade-in">
            {/* Cards en 2 columnas */}
            <div className="grid grid-cols-2 gap-2">
              {CASOS_CONFIG.map(caso => {
                const open      = abierto === caso.key
                const guardando = guardandoRuta === caso.key
                const guardado  = guardadoRuta  === caso.key

                return (
                  <div
                    key={caso.key}
                    className="rounded-xl border transition-all duration-200 overflow-hidden"
                    style={{
                      borderColor: open ? `${caso.color}50` : '#e2e8f0',
                      backgroundColor: open ? `${caso.color}06` : 'white',
                    }}
                  >
                    {/* Header */}
                    <button
                      onClick={() => abrirCaso(caso.key)}
                      className="w-full flex items-center gap-2.5 px-3 py-2.5 text-left"
                    >
                      <div
                        className="w-6 h-6 rounded-md flex items-center justify-center flex-shrink-0"
                        style={{ backgroundColor: `${caso.color}18` }}
                      >
                        <div className="w-2 h-2 rounded-full" style={{ backgroundColor: caso.color }} />
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-1.5">
                          <span className="text-xs font-semibold text-slate-800">{caso.label}</span>
                          {guardado && <CheckCircle2 size={10} className="text-emerald-500 flex-shrink-0" />}
                        </div>
                        {!open && (
                          <p className="text-xs text-slate-400 font-mono truncate mt-0.5 leading-tight">
                            {rutaCorta(getRutaActual(caso.key, 'compartida'))}
                          </p>
                        )}
                      </div>
                      <ChevronDown
                        size={13}
                        className={`text-slate-400 flex-shrink-0 transition-transform duration-200 ${open ? 'rotate-180' : ''}`}
                      />
                    </button>

                    {/* Expandido */}
                    {open && (
                      <div
                        className="px-3 pb-3 space-y-2 border-t animate-fade-in"
                        style={{ borderColor: `${caso.color}20` }}
                      >
                        <div className="pt-2">
                          <label className="flex items-center gap-1 text-xs font-semibold mb-1" style={{ color: caso.color }}>
                            <Network size={10} /> Red compartida
                          </label>
                          <input
                            type="text"
                            value={rutasEdit[getEditKey(caso.key, 'compartida')] ?? getRutaActual(caso.key, 'compartida')}
                            onChange={e => setRutasEdit(prev => ({ ...prev, [getEditKey(caso.key, 'compartida')]: e.target.value }))}
                            className="w-full rounded-lg border px-2.5 py-1.5 text-xs font-mono text-slate-700 focus:outline-none transition-colors"
                            style={{ borderColor: `${caso.color}40`, backgroundColor: `${caso.color}06` }}
                            placeholder="\\servidor\ruta\..."
                          />
                        </div>
                        <p className="text-xs text-slate-400">
                          Auto: <span className="font-mono">\{anioStr}\{mesStr}\{diaStr}</span>
                        </p>
                        <div className="flex items-center gap-1.5">
                          <button
                            onClick={() => guardarRutaCaso(caso.key)}
                            disabled={!!guardando}
                            className="flex items-center gap-1 rounded-lg px-2.5 py-1 text-xs font-semibold text-white disabled:opacity-60"
                            style={{ backgroundColor: caso.color }}
                          >
                            {guardando ? <span className="spinner" /> : <Save size={10} />}
                            Guardar
                          </button>
                          <button
                            onClick={() => setAbierto(null)}
                            className="text-xs px-2.5 py-1 rounded-lg bg-slate-100 text-slate-500 hover:bg-slate-200 transition-colors"
                          >
                            Cancelar
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </div>

    </div>
  )
}