'use client'

import { useRef, useState, useEffect, DragEvent } from 'react'
import { Upload, Download, FileText, X, HardDrive, CheckCircle2, Loader2, ChevronRight, Activity } from 'lucide-react'
import { CASOS, CasoKey, ResultadoProceso, API } from '../lib/api'
import { getToken } from '../hooks/useAuth'

interface ProgressStep {
  step: string
  elapsed: number
  done?: boolean
  result?: ResultadoProceso
  error?: string
}

function ProgressPanel({ steps, color, finalizado }: { steps: ProgressStep[]; color: string; finalizado: boolean }) {
  const [elapsed, setElapsed] = useState(0)
  const startRef = useRef(Date.now())

  useEffect(() => {
    if (finalizado) return
    const id = setInterval(() => setElapsed((Date.now() - startRef.current) / 1000), 100)
    return () => clearInterval(id)
  }, [finalizado])

  const tiempo = finalizado ? (steps[steps.length - 1]?.elapsed ?? 0) : elapsed

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
          {finalizado ? 'Completado' : 'Procesando'}
        </span>
        <span className="font-mono text-xs font-bold" style={{ color }}>{tiempo.toFixed(1)}s</span>
      </div>
      {steps.map((s, i) => {
        const isCurrent = i === steps.length - 1 && !finalizado
        return (
          <div key={i} className="flex items-center gap-2">
            {s.error
              ? <X size={11} className="text-red-500 flex-shrink-0" />
              : isCurrent
                ? <Loader2 size={11} className="animate-spin flex-shrink-0" style={{ color }} />
                : <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: `${color}30`, border: `1.5px solid ${color}` }} />
            }
            <span className={`text-xs flex-1 ${isCurrent ? 'font-medium' : 'text-slate-400'}`} style={isCurrent ? { color } : {}}>
              {s.step}
            </span>
            {!isCurrent && <span className="text-xs text-slate-300 font-mono">{s.elapsed.toFixed(1)}s</span>}
            {isCurrent && <span className="text-xs font-mono font-bold" style={{ color }}>{elapsed.toFixed(1)}s</span>}
          </div>
        )
      })}
    </div>
  )
}

function ResultadoCard({ resultado, guardarLocal, color }: { resultado: ResultadoProceso; guardarLocal: boolean; color: string }) {
  const descargar = async (path: string, nombre: string) => {
    const token = getToken()
    const res = await fetch(`${API}/descargar?path=${encodeURIComponent(path)}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    })
    if (!res.ok) return
    const blob = await res.blob()
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = nombre; a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-2">
        {[
          { label: 'Entrada',    value: resultado.total_entrada,    bg: `${color}08`,  text: color     },
          { label: 'Carga',      value: resultado.total_carga,      bg: '#10b98110',   text: '#10b981' },
          { label: 'Repetidos',  value: resultado.total_repetidos,  bg: '#f59e0b10',   text: '#f59e0b' },
          { label: 'Bloqueados', value: resultado.total_bloqueados, bg: '#ef444410',   text: '#ef4444' },
        ].map(({ label, value, bg, text }) => (
          <div key={label} className="rounded-xl px-3 py-2.5 flex items-center justify-between" style={{ backgroundColor: bg }}>
            <span className="text-xs text-slate-500">{label}</span>
            <span className="text-base font-bold" style={{ color: text }}>{value}</span>
          </div>
        ))}
      </div>

      {resultado.archivos && resultado.archivos.length > 0 && (
        <div className="space-y-1.5">
          {guardarLocal ? (
            <p className="text-xs text-slate-400 flex items-center gap-1.5 py-1">
              <HardDrive size={11} /> Guardado en carpeta local
            </p>
          ) : (
            resultado.archivos.map(a => (
              <button
                key={a.nombre}
                onClick={() => descargar(a.path, a.nombre)}
                className="flex items-center gap-2 rounded-lg px-3 py-2 text-sm w-full text-left transition-all hover:opacity-75"
                style={{ backgroundColor: `${color}08`, border: `1px solid ${color}20`, color }}
              >
                <FileText size={12} />
                <span className="flex-1 truncate text-xs font-medium">{a.nombre}</span>
                <Download size={11} className="opacity-60 flex-shrink-0" />
              </button>
            ))
          )}
        </div>
      )}
    </div>
  )
}

export function CasoCard({ casoKey }: { casoKey: CasoKey }) {
  const cfg = CASOS[casoKey]
  const color = cfg.color

  const [phase, setPhase] = useState<'idle' | 'loading' | 'done'>('idle')
  const [resultado, setResultado] = useState<ResultadoProceso | null>(null)
  const [archivo, setArchivo] = useState<File | null>(null)
  const [dragging, setDragging] = useState(false)
  const [guardarLocal, setGuardarLocal] = useState(false)
  const [steps, setSteps] = useState<ProgressStep[]>([])
  const [finalizado, setFinalizado] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    const token = getToken()
    fetch(`${API}/config/general`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    }).then(r => r.json()).then(d => setGuardarLocal(d.guardar_local ?? false)).catch(() => {})
  }, [])

  const handleProcesar = async (file?: File) => {
    setPhase('loading')
    setResultado(null)
    setError(null)
    setFinalizado(false)
    setSteps([{ step: 'Iniciando...', elapsed: 0 }])

    try {
      const token = getToken()
      const authHeader = token ? { Authorization: `Bearer ${token}` } : {}

      let res: Response
      if (file) {
        const form = new FormData()
        form.append('file', file)
        res = await fetch(`${API}/procesar/${casoKey.toLowerCase()}`, { method: 'POST', headers: authHeader, body: form })
      } else {
        res = await fetch(`${API}/procesar/${casoKey.toLowerCase()}`, { method: 'POST', headers: authHeader })
      }

      if (!res.ok) {
        const err = await res.json()
        throw new Error(err.detail || 'Error al procesar')
      }

      const { job_id } = await res.json()
      const es = new EventSource(`${API}/jobs/${job_id}/stream`)
      esRef.current = es

      es.onmessage = (e) => {
        if (!e.data || e.data.startsWith(':')) return
        try {
          const msg: ProgressStep = JSON.parse(e.data)
          setSteps(prev => {
            const base = prev.length === 1 && prev[0].step === 'Iniciando...' ? [] : prev
            return [...base, { step: msg.step, elapsed: msg.elapsed }]
          })
          if (msg.done) {
            es.close(); esRef.current = null
            setFinalizado(true)
            if (msg.error) { setError(msg.error); setPhase('done') }
            else { setResultado(msg.result ?? null); setPhase('done') }
          }
        } catch {}
      }

      es.onerror = () => {
        es.close(); esRef.current = null
        setFinalizado(true); setPhase('done'); setError('Error de conexión')
      }
    } catch (e: any) {
      setError(e.message); setPhase('done'); setFinalizado(true); setSteps([])
    }
  }

  const reset = () => {
    setPhase('idle'); setArchivo(null); setResultado(null)
    setError(null); setSteps([]); setFinalizado(false)
    esRef.current?.close(); esRef.current = null
  }

  const handleDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault(); setDragging(false)
    const file = e.dataTransfer.files?.[0]
    if (file) { setArchivo(file); setResultado(null); setPhase('idle') }
  }

  return (
    <div
      className="rounded-2xl overflow-hidden w-full relative"
      style={{
        background: `linear-gradient(135deg, ${color}08 0%, white 60%)`,
        border: `1px solid ${color}25`,
        boxShadow: `0 4px 24px ${color}10, 0 1px 3px rgba(0,0,0,0.06)`,
      }}
    >
      {/* Círculo decorativo */}
      <div
        className="absolute top-0 right-0 w-36 h-36 rounded-full opacity-[0.06] pointer-events-none"
        style={{ backgroundColor: color, transform: 'translate(30%, -30%)' }}
      />

      <div className="relative p-5 space-y-4">
        {/* Header — siempre visible */}
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <Activity size={13} style={{ color }} />
              <p className="text-xs font-bold uppercase tracking-widest" style={{ color }}>{cfg.label}</p>
            </div>
            <p className="text-slate-500 text-xs leading-relaxed">{cfg.descripcion}</p>
          </div>

          {phase === 'loading' && (
            <Loader2 size={14} className="animate-spin mt-0.5 flex-shrink-0" style={{ color }} />
          )}
          {phase === 'done' && !error && steps.length > 0 && (
            <div
              className="flex items-center gap-1 rounded-full px-2.5 py-1 flex-shrink-0"
              style={{ backgroundColor: '#10b98110', border: '1px solid #10b98130' }}
            >
              <CheckCircle2 size={11} className="text-emerald-500" />
              <span className="text-xs font-semibold text-emerald-600">
                {steps[steps.length - 1]?.elapsed.toFixed(1)}s
              </span>
            </div>
          )}
        </div>

        {/* IDLE: upload */}
        {phase === 'idle' && (
          <div className="space-y-2 animate-fade-in">
            {cfg.sftp ? (
              <button
                onClick={() => handleProcesar()}
                className="w-full py-2.5 rounded-xl text-white text-sm font-semibold flex items-center justify-center gap-2 transition-all"
                style={{ background: `linear-gradient(135deg, ${color}, ${color}cc)`, boxShadow: `0 4px 12px ${color}30` }}
              >
                <ChevronRight size={15} /> Descargar desde SFTP
              </button>
            ) : (
              <>
                <div
                  onDrop={handleDrop}
                  onDragOver={e => { e.preventDefault(); setDragging(true) }}
                  onDragLeave={() => setDragging(false)}
                  onClick={() => !archivo && fileRef.current?.click()}
                  className="rounded-xl border-2 border-dashed flex items-center gap-3 px-4 py-3 transition-all"
                  style={{
                    borderColor: dragging ? color : `${color}30`,
                    backgroundColor: dragging ? `${color}08` : 'transparent',
                    cursor: archivo ? 'default' : 'pointer',
                  }}
                >
                  {archivo ? (
                    <>
                      <FileText size={15} style={{ color }} className="flex-shrink-0" />
                      <span className="text-sm text-slate-600 flex-1 truncate">{archivo.name}</span>
                      <button
                        onClick={e => { e.stopPropagation(); setArchivo(null) }}
                        className="text-slate-300 hover:text-slate-500 transition-colors"
                      >
                        <X size={13} />
                      </button>
                    </>
                  ) : (
                    <>
                      <Upload size={15} className="text-slate-300 flex-shrink-0" />
                      <span className="text-sm text-slate-400">
                        {dragging ? 'Suelta aquí' : 'Arrastra o selecciona un archivo'}
                      </span>
                      <span className="text-xs text-slate-300 ml-auto">.xls .xlsx .csv</span>
                    </>
                  )}
                </div>
                <input
                  ref={fileRef} type="file" accept=".xls,.xlsx,.xlsm,.csv" className="hidden"
                  onChange={e => { const f = e.target.files?.[0]; if (f) setArchivo(f) }}
                />
                <button
                  onClick={() => archivo && handleProcesar(archivo)}
                  disabled={!archivo}
                  className="w-full py-2.5 rounded-xl text-white text-sm font-semibold disabled:opacity-40 transition-all flex items-center justify-center gap-2"
                  style={{
                    background: archivo ? `linear-gradient(135deg, ${color}, ${color}cc)` : '#e2e8f0',
                    boxShadow: archivo ? `0 4px 12px ${color}30` : 'none',
                    color: archivo ? 'white' : '#94a3b8',
                  }}
                >
                  <ChevronRight size={15} /> Procesar
                </button>
              </>
            )}
          </div>
        )}

        {/* LOADING: solo progreso */}
        {phase === 'loading' && steps.length > 0 && (
          <div className="animate-fade-in">
            <ProgressPanel steps={steps} color={color} finalizado={false} />
          </div>
        )}

        {/* DONE: progreso + resultado + botón reset */}
        {phase === 'done' && (
          <div className="animate-fade-in space-y-4">
            {steps.length > 0 && (
              <ProgressPanel steps={steps} color={color} finalizado={true} />
            )}

            {error ? (
              <div
                className="rounded-xl px-3 py-2.5 text-xs text-red-600"
                style={{ backgroundColor: '#fef2f2', border: '1px solid #fecaca' }}
              >
                <span className="font-semibold">Error: </span>{error}
              </div>
            ) : resultado ? (
              <ResultadoCard resultado={resultado} guardarLocal={guardarLocal} color={color} />
            ) : null}

            <button
              onClick={reset}
              className="w-full text-xs py-2 rounded-xl transition-all font-medium"
              style={{ color, backgroundColor: `${color}08`, border: `1px solid ${color}20` }}
            >
              ← Nuevo proceso
            </button>
          </div>
        )}
      </div>
    </div>
  )
}