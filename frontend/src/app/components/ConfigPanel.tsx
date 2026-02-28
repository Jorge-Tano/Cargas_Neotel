'use client'

import { useEffect, useState } from 'react'
import { Folder, Save, ChevronRight, CheckCircle2 } from 'lucide-react'
import { CASOS, CasoKey, getRutaBase, setRutaBase } from '../lib/api'

export function ConfigPanel() {
  const [rutaBase, setRutaBaseState] = useState('')
  const [rutasInd, setRutasInd] = useState<Record<string, string>>({
    SAV: '', AV: '', REFI: '', PL: '', PERDIDAS: ''
  })
  const [guardando, setGuardando] = useState(false)
  const [guardado, setGuardado] = useState(false)
  const [usarInd, setUsarInd] = useState(false)

  useEffect(() => {
    getRutaBase().then(r => setRutaBaseState(r))
  }, [])

  const guardar = async () => {
    setGuardando(true)
    try {
      await setRutaBase(rutaBase)
      setGuardado(true)
      setTimeout(() => setGuardado(false), 2500)
    } finally {
      setGuardando(false)
    }
  }

  const rutaEjemplo = rutaBase || 'D:\\Cargas\\Leakage'

  return (
    <div className="space-y-6 max-w-2xl animate-fade-in">

      {/* Ruta base */}
      <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
        <div className="flex items-center gap-3 px-5 py-4 border-b border-slate-100">
          <div className="w-8 h-8 rounded-lg bg-slate-100 flex items-center justify-center">
            <Folder size={15} className="text-slate-500" />
          </div>
          <div>
            <h3 className="font-semibold text-slate-800">Ruta de salida</h3>
            <p className="text-xs text-slate-400">Carpeta raíz donde se guardarán todos los archivos generados</p>
          </div>
        </div>

        <div className="px-5 py-5 space-y-5">
          {/* Input ruta base */}
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1.5">
              Ruta base (todos los casos)
            </label>
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

          {/* Rutas individuales */}
          <div>
            <button
              onClick={() => setUsarInd(!usarInd)}
              className="flex items-center gap-1.5 text-sm text-blue-600 hover:text-blue-700 font-medium"
            >
              <ChevronRight size={14} className={`transition-transform ${usarInd ? 'rotate-90' : ''}`} />
              {usarInd ? 'Ocultar' : 'Configurar'} rutas individuales por caso
            </button>

            {usarInd && (
              <div className="mt-4 space-y-3 animate-fade-in">
                <p className="text-xs text-slate-500">
                  Si defines una ruta individual tiene prioridad sobre la base. Deja vacío para usar la base.
                </p>
                {(Object.keys(CASOS) as CasoKey[]).map(k => (
                  <div key={k} className="flex items-center gap-3">
                    <span
                      className="w-24 text-sm font-semibold flex-shrink-0"
                      style={{ color: CASOS[k].color }}
                    >
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

          {/* Guardar */}
          <div className="flex items-center gap-3 pt-2 border-t border-slate-100">
            <button
              onClick={guardar}
              disabled={guardando}
              className="flex items-center gap-2 rounded-xl px-5 py-2.5 text-sm font-medium text-white transition-colors disabled:opacity-60"
              style={{ backgroundColor: '#0c90e6' }}
            >
              {guardando ? <span className="spinner" /> : <Save size={14} />}
              Guardar
            </button>
            {guardado && (
              <span className="flex items-center gap-1.5 text-sm text-emerald-600 animate-fade-in">
                <CheckCircle2 size={14} /> Guardado
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Preview estructura */}
      <div className="rounded-2xl border border-slate-200 bg-white shadow-sm p-5">
        <h3 className="font-semibold text-slate-800 mb-3">Vista previa de estructura</h3>
        <div className="rounded-xl bg-slate-50 p-4 font-mono text-xs text-slate-600 leading-6">
          <span className="text-slate-800 font-semibold">{rutaEjemplo}\</span><br />
          {(Object.keys(CASOS) as CasoKey[]).map((k, i, arr) => {
            const isLast = i === arr.length - 1
            const prefix = isLast ? '└─' : '├─'
            const indent = isLast ? '   ' : '│  '
            return (
              <span key={k}>
                {prefix} <span style={{ color: CASOS[k].color }} className="font-semibold">{k}</span>\<br />
                {indent} └─ 2026\<br />
                {indent}    └─ 02-Febrero\<br />
                {indent}       └─ <span className="text-slate-400">28\  ← archivos aquí</span><br />
              </span>
            )
          })}
        </div>
      </div>
    </div>
  )
}