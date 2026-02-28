'use client'

import { useRef, useState } from 'react'
import { Upload, Download, FileText } from 'lucide-react'
import { EstadoBadge, EstadoProceso } from './EstadoBadge'
import { CASOS, CasoKey, ResultadoProceso, API, procesarCaso } from '../lib/api'

function ResultadoCard({ resultado }: { resultado: ResultadoProceso }) {
  return (
    <div className="rounded-xl border border-slate-100 bg-slate-50 p-4 animate-fade-in">
      <div className="grid grid-cols-4 gap-3 mb-4">
        {[
          { label: 'Entrada',    value: resultado.total_entrada,    color: 'text-slate-700' },
          { label: 'Repetidos',  value: resultado.total_repetidos,  color: 'text-amber-600' },
          { label: 'Bloqueados', value: resultado.total_bloqueados, color: 'text-red-500' },
          { label: 'Carga',      value: resultado.total_carga,      color: 'text-emerald-600' },
        ].map(({ label, value, color }) => (
          <div key={label} className="text-center">
            <p className={`text-2xl font-bold ${color}`}>{value}</p>
            <p className="text-xs text-slate-500 mt-0.5">{label}</p>
          </div>
        ))}
      </div>
      {resultado.archivos && resultado.archivos.length > 0 && (
        <div className="space-y-1.5">
          {resultado.archivos.map(a => (
            <a
              key={a.nombre}
              href={`${API}/descargar?path=${encodeURIComponent(a.path)}`}
              className="flex items-center gap-2 rounded-lg border border-blue-100 bg-white px-3 py-2 text-sm text-blue-700 hover:bg-blue-50 transition-colors"
              download
            >
              <FileText size={13} />
              {a.nombre}
              <Download size={12} className="ml-auto opacity-50" />
            </a>
          ))}
        </div>
      )}
    </div>
  )
}

export function CasoCard({ casoKey }: { casoKey: CasoKey }) {
  const cfg = CASOS[casoKey]
  const [estado, setEstado] = useState<EstadoProceso>('idle')
  const [resultado, setResultado] = useState<ResultadoProceso | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  const handleProcesar = async (file?: File) => {
    setEstado('loading')
    setResultado(null)
    try {
      const res = await procesarCaso(casoKey, file)
      setResultado(res)
      setEstado('success')
    } catch (e: any) {
      setResultado({ total_entrada: 0, total_repetidos: 0, total_bloqueados: 0, total_carga: 0, error: e.message })
      setEstado('error')
    }
  }

  return (
    <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-4" style={{ borderLeft: `4px solid ${cfg.color}` }}>
        <div>
          <h3 className="font-semibold text-slate-800">{cfg.label}</h3>
          <p className="text-xs text-slate-400 mt-0.5">{cfg.descripcion}</p>
        </div>
        <EstadoBadge estado={estado} />
      </div>

      {/* Acción */}
      <div className="px-5 py-4 border-t border-slate-100">
        {cfg.sftp ? (
          <button
            onClick={() => handleProcesar()}
            disabled={estado === 'loading'}
            className="flex items-center justify-center gap-2 rounded-xl px-4 py-2.5 text-sm font-medium text-white w-full transition-all disabled:opacity-60"
            style={{ backgroundColor: cfg.color }}
          >
            {estado === 'loading' ? <span className="spinner" /> : <Download size={15} />}
            Descargar desde SFTP
          </button>
        ) : (
          <>
            <button
              onClick={() => fileRef.current?.click()}
              disabled={estado === 'loading'}
              className="flex items-center justify-center gap-2 rounded-xl px-4 py-2.5 text-sm font-medium text-white w-full transition-all disabled:opacity-60"
              style={{ backgroundColor: cfg.color }}
            >
              {estado === 'loading' ? <span className="spinner" /> : <Upload size={15} />}
              Subir archivo
            </button>
            <input
              ref={fileRef}
              type="file"
              accept=".xls,.xlsx,.xlsm,.csv"
              className="hidden"
              onChange={e => { const f = e.target.files?.[0]; if (f) handleProcesar(f) }}
            />
          </>
        )}
      </div>

      {/* Resultado */}
      {resultado && (
        <div className="px-5 pb-5">
          {resultado.error ? (
            <div className="rounded-xl border border-red-100 bg-red-50 p-3 text-sm text-red-600 animate-fade-in">
              <span className="font-medium">Error:</span> {resultado.error}
            </div>
          ) : (
            <ResultadoCard resultado={resultado} />
          )}
        </div>
      )}
    </div>
  )
}