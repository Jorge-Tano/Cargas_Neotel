'use client'

import { useRef, useState, DragEvent } from 'react'
import { Upload, Download, FileText, X } from 'lucide-react'
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

function FilePreview({ file, onClear }: { file: File; onClear: () => void }) {
  const kb = (file.size / 1024).toFixed(1)
  const mb = (file.size / 1024 / 1024).toFixed(2)
  const size = file.size > 1024 * 1024 ? `${mb} MB` : `${kb} KB`
  const fecha = new Date(file.lastModified).toLocaleString('es-CL')

  return (
    <div className="flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2.5 animate-fade-in">
      <FileText size={16} className="text-slate-400 flex-shrink-0" />
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-slate-700 truncate">{file.name}</p>
        <p className="text-xs text-slate-400">{size} · {fecha}</p>
      </div>
      <button
        onClick={onClear}
        className="p-1 rounded-md hover:bg-slate-200 text-slate-400 hover:text-slate-600 transition-colors"
      >
        <X size={13} />
      </button>
    </div>
  )
}

export function CasoCard({ casoKey }: { casoKey: CasoKey }) {
  const cfg = CASOS[casoKey]
  const [estado, setEstado] = useState<EstadoProceso>('idle')
  const [resultado, setResultado] = useState<ResultadoProceso | null>(null)
  const [archivoSeleccionado, setArchivoSeleccionado] = useState<File | null>(null)
  const [dragging, setDragging] = useState(false)
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

  const handleFileChange = (file: File) => {
    setArchivoSeleccionado(file)
    setResultado(null)
    setEstado('idle')
  }

  const handleDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    setDragging(false)
    const file = e.dataTransfer.files?.[0]
    if (file) handleFileChange(file)
  }

  const handleDragOver = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    setDragging(true)
  }

  const handleDragLeave = () => setDragging(false)

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
      <div className="px-5 py-4 border-t border-slate-100 space-y-3">
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
            {/* Zona drag & drop */}
            <div
              onDrop={handleDrop}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onClick={() => fileRef.current?.click()}
              className={`relative flex flex-col items-center justify-center gap-1.5 rounded-xl border-2 border-dashed px-4 py-5 cursor-pointer transition-all
                ${dragging
                  ? 'border-blue-400 bg-blue-50'
                  : 'border-slate-200 hover:border-slate-300 hover:bg-slate-50'
                }`}
            >
              <Upload size={18} className={dragging ? 'text-blue-500' : 'text-slate-300'} />
              <p className="text-xs text-slate-400">
                {dragging ? 'Suelta el archivo aquí' : 'Arrastra un archivo o haz clic para seleccionar'}
              </p>
              <p className="text-xs text-slate-300">.xls · .xlsx · .xlsm · .csv</p>
            </div>

            <input
              ref={fileRef}
              type="file"
              accept=".xls,.xlsx,.xlsm,.csv"
              className="hidden"
              onChange={e => { const f = e.target.files?.[0]; if (f) handleFileChange(f) }}
            />

            {/* Preview del archivo seleccionado */}
            {archivoSeleccionado && (
              <FilePreview
                file={archivoSeleccionado}
                onClear={() => { setArchivoSeleccionado(null); setResultado(null); setEstado('idle') }}
              />
            )}

            {/* Botón procesar */}
            <button
              onClick={() => archivoSeleccionado && handleProcesar(archivoSeleccionado)}
              disabled={!archivoSeleccionado || estado === 'loading'}
              className="flex items-center justify-center gap-2 rounded-xl px-4 py-2.5 text-sm font-medium text-white w-full transition-all disabled:opacity-40"
              style={{ backgroundColor: cfg.color }}
            >
              {estado === 'loading' ? <span className="spinner" /> : <Upload size={15} />}
              Procesar archivo
            </button>
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