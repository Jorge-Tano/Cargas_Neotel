'use client'

import { useEffect, useState } from 'react'
import { Folder, Save, ChevronRight, CheckCircle2, Database } from 'lucide-react'
import { CASOS, CasoKey, getRutaBase, setRutaBase, API } from '../lib/api'

interface IddatabaseConfig {
  IDDATABASE_SAV: number
  IDDATABASE_AV: number
  IDDATABASE_PL: number
  IDDATABASE_REFI: number
}

const ID_LABELS: { key: keyof IddatabaseConfig; caso: CasoKey; label: string }[] = [
  { key: 'IDDATABASE_SAV', caso: 'SAV', label: 'SAV' },
  { key: 'IDDATABASE_AV', caso: 'AV', label: 'AV' },
  { key: 'IDDATABASE_PL', caso: 'PL', label: 'Pago Liviano' },
  { key: 'IDDATABASE_REFI', caso: 'REFI', label: 'REFI' },
]

export function ConfigPanel() {
  // Ruta
  const [rutaBase, setRutaBaseState] = useState('')
  const [usarInd, setUsarInd] = useState(false)
  const [rutasInd, setRutasInd] = useState<Record<string, string>>({ SAV: '', AV: '', REFI: '', PL: '', PERDIDAS: '' })
  const [guardandoRuta, setGuardandoRuta] = useState(false)
  const [guardadoRuta, setGuardadoRuta] = useState(false)

  // IDs BD
  const [ids, setIds] = useState<IddatabaseConfig>({
    IDDATABASE_SAV: 218, IDDATABASE_AV: 92, IDDATABASE_PL: 131, IDDATABASE_REFI: 70
  })
  const [guardandoIds, setGuardandoIds] = useState(false)
  const [guardadoIds, setGuardadoIds] = useState(false)

  useEffect(() => {
    getRutaBase().then(r => setRutaBaseState(r))
    fetch(`${API}/config/iddatabase`).then(r => r.json()).then(d => setIds(d))
  }, [])

  const guardarRuta = async () => {
    setGuardandoRuta(true)
    try {
      await setRutaBase(rutaBase)
      setGuardadoRuta(true)
      setTimeout(() => setGuardadoRuta(false), 2500)
    } finally {
      setGuardandoRuta(false)
    }
  }

  const guardarIds = async () => {
    setGuardandoIds(true)
    try {
      await fetch(`${API}/config/iddatabase`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(ids),
      })
      setGuardadoIds(true)
      setTimeout(() => setGuardadoIds(false), 2500)
    } finally {
      setGuardandoIds(false)
    }
  }

  const rutaEjemplo = rutaBase || 'D:\\Cargas\\Leakage'

  return (
    <div className="space-y-6 max-w-2xl animate-fade-in">

      {/* ── Sección IDDATABASE ─────────────────────────── */}
      <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
        <div className="flex items-center gap-3 px-5 py-4 border-b border-slate-100">
          <div className="w-8 h-8 rounded-lg bg-blue-50 flex items-center justify-center">
            <Database size={15} className="text-blue-500" />
          </div>
          <div>
            <h3 className="font-semibold text-slate-800">IDs de base de datos</h3>
            <p className="text-xs text-slate-400">Se actualizan mensualmente en Neotel</p>
          </div>
        </div>

        <div className="px-5 py-5 space-y-4">
          <p className="text-xs text-slate-500">
            Corresponde al <span className="font-mono bg-slate-100 px-1 rounded">IDDATABASE</span> usado
            en las consultas de repetidos. Actualiza estos valores al inicio de cada mes.
          </p>

          <div className="space-y-3">
            {ID_LABELS.map(({ key, caso, label }) => (
              <div key={key} className="flex items-center gap-4">
                <div className="flex items-center gap-2 w-32">
                  <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: CASOS[caso].color }} />
                  <span className="text-sm font-medium text-slate-700">{label}</span>
                </div>
                <input
                  type="number"
                  value={ids[key]}
                  onChange={e => setIds(prev => ({ ...prev, [key]: parseInt(e.target.value) || 0 }))}
                  className="w-32 rounded-xl border border-slate-200 px-3 py-2 text-sm font-mono text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-200 text-center"
                />
                <span className="text-xs text-slate-400 font-mono">WHERE b.IDDATABASE = {ids[key]}</span>
              </div>
            ))}
          </div>

          <div className="flex items-center gap-3 pt-2 border-t border-slate-100">
            <button
              onClick={guardarIds}
              disabled={guardandoIds}
              className="flex items-center gap-2 rounded-xl px-5 py-2.5 text-sm font-medium text-white transition-colors disabled:opacity-60"
              style={{ backgroundColor: '#0c90e6' }}
            >
              {guardandoIds ? <span className="spinner" /> : <Save size={14} />}
              Guardar IDs
            </button>
            {guardadoIds && (
              <span className="flex items-center gap-1.5 text-sm text-emerald-600 animate-fade-in">
                <CheckCircle2 size={14} /> Guardado
              </span>
            )}
          </div>
        </div>
      </div>

      {/* ── Sección Ruta de salida ──────────────────────── */}
      <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
        <div className="flex items-center gap-3 px-5 py-4 border-b border-slate-100">
          <div className="w-8 h-8 rounded-lg bg-slate-100 flex items-center justify-center">
            <Folder size={15} className="text-slate-500" />
          </div>
          <div>
            <h3 className="font-semibold text-slate-800">Ruta de salida</h3>
            <p className="text-xs text-slate-400">Carpeta raíz donde se guardarán los archivos generados</p>
          </div>
        </div>

        <div className="px-5 py-5 space-y-5">
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1.5">Ruta base (todos los casos)</label>
            <input
              type="text"
              value={rutaBase}
              onChange={e => setRutaBaseState(e.target.value)}
              placeholder="D:\Cargas\Leakage"
              className="w-full rounded-xl border border-slate-200 px-3 py-2.5 text-sm font-mono text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
            <p className="text-xs text-slate-400 mt-1.5">
              Estructura automática: <span className="font-mono">[Ruta]\[CASO]\[AÑO]\[MES]\[DÍA]\</span>
            </p>
          </div>

          <div>
            <button onClick={() => setUsarInd(!usarInd)}
              className="flex items-center gap-1.5 text-sm text-blue-600 hover:text-blue-700 font-medium">
              <ChevronRight size={14} className={`transition-transform ${usarInd ? 'rotate-90' : ''}`} />
              {usarInd ? 'Ocultar' : 'Configurar'} rutas individuales por caso
            </button>
            {usarInd && (
              <div className="mt-4 space-y-3 animate-fade-in">
                <p className="text-xs text-slate-500">Deja vacío para usar la ruta base.</p>
                {(Object.keys(CASOS) as CasoKey[]).map(k => (
                  <div key={k} className="flex items-center gap-3">
                    <span className="w-24 text-sm font-semibold flex-shrink-0" style={{ color: CASOS[k].color }}>
                      {CASOS[k].label}
                    </span>
                    <input
                      type="text"
                      value={rutasInd[k]}
                      onChange={e => setRutasInd(prev => ({ ...prev, [k]: e.target.value }))}
                      placeholder={`${rutaEjemplo}\\${k}\\...`}
                      className="flex-1 rounded-xl border border-slate-200 px-3 py-2 text-sm font-mono text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-200"
                    />
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="flex items-center gap-3 pt-2 border-t border-slate-100">
            <button onClick={guardarRuta} disabled={guardandoRuta}
              className="flex items-center gap-2 rounded-xl px-5 py-2.5 text-sm font-medium text-white transition-colors disabled:opacity-60"
              style={{ backgroundColor: '#0c90e6' }}>
              {guardandoRuta ? <span className="spinner" /> : <Save size={14} />}
              Guardar ruta
            </button>
            {guardadoRuta && (
              <span className="flex items-center gap-1.5 text-sm text-emerald-600 animate-fade-in">
                <CheckCircle2 size={14} /> Guardado
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}