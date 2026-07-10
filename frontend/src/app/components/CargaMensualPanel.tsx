'use client'

import { useState, useEffect, useRef } from 'react'
import {
  Upload, Download, FileText, X, Loader2, ChevronRight, Activity,
  Folder, File as FileIcon, ArrowLeft,
} from 'lucide-react'
import { API } from '../lib/api'
import { getToken } from '../hooks/useAuth'

interface ProgressStep {
  step: string
  elapsed: number
}

interface ResultadoCargaMensual {
  total_entrada: number
  total_sin_cruce: number
  total_eliminados: number
  total_no_cargados_comunas: number
  total_carga: number
  aaaamm: string
  archivos: { nombre: string; path: string }[]
}

interface FtpEntrada {
  nombre: string
  es_dir: boolean
  tamano: number
  mtime: number
}

function authHeaders(): Record<string, string> {
  const token = getToken()
  return token ? { Authorization: `Bearer ${token}` } : {}
}

function labelArchivo(nombre: string): string {
  const n = nombre.toLowerCase()
  if (n.startsWith('update')) return 'Update'
  if (n.startsWith('detallecarga')) return 'DetalleCarga'
  if (n.startsWith('eliminar')) return 'Eliminar.txt'
  return nombre.replace(/\.[^.]+$/, '')
}

// ─── Explorador FTP simple (breadcrumb + lista) ───────────────────────────
function FtpBrowser({
  servidor,
  rutaInicial,
  onSeleccionar,
  onCerrar,
  color,
}: {
  servidor: 'principal' | 'neotel17'
  rutaInicial: string
  onSeleccionar: (ruta: string) => void
  onCerrar: () => void
  color: string
}) {
  const [ruta, setRuta] = useState(rutaInicial)
  const [entradas, setEntradas] = useState<FtpEntrada[]>([])
  const [cargando, setCargando] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const endpoint = servidor === 'principal' ? 'ftp-principal' : 'ftp-neotel17'

  const cargar = async (nuevaRuta: string) => {
    setCargando(true)
    setError(null)
    try {
      const res = await fetch(`${API}/carga-mensual/${endpoint}/listar?ruta=${encodeURIComponent(nuevaRuta)}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || 'Error al listar')
      const data = await res.json()
      setRuta(data.ruta)
      setEntradas(data.entradas ?? [])
    } catch (e: any) {
      setError(e.message)
      setEntradas([])
    } finally {
      setCargando(false)
    }
  }

  useEffect(() => { cargar(rutaInicial) }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const subirNivel = () => {
    const partes = ruta.split('/').filter(Boolean)
    partes.pop()
    cargar('/' + partes.join('/'))
  }

  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3 space-y-2 shadow-lg absolute z-20 w-full max-h-80 flex flex-col">
      <div className="flex items-center gap-2">
        <button onClick={subirNivel} className="text-slate-400 hover:text-slate-600 flex-shrink-0">
          <ArrowLeft size={14} />
        </button>
        <input
          value={ruta}
          onChange={e => setRuta(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && cargar(ruta)}
          className="flex-1 text-xs px-2 py-1 rounded-lg border border-slate-200 font-mono min-w-0"
        />
        <button onClick={onCerrar} className="text-slate-400 hover:text-slate-600 flex-shrink-0">
          <X size={14} />
        </button>
      </div>
      <div className="flex-1 overflow-y-auto space-y-0.5">
        {cargando && <div className="text-xs text-slate-400 py-2 text-center">Cargando...</div>}
        {error && <div className="text-xs text-red-500 py-2 px-1">{error}</div>}
        {!cargando && !error && entradas.length === 0 && (
          <div className="text-xs text-slate-400 py-2 text-center">Carpeta vacía</div>
        )}
        {entradas.map(en => (
          <button
            key={en.nombre}
            onClick={() => en.es_dir ? cargar(`${ruta}/${en.nombre}`) : onSeleccionar(`${ruta}/${en.nombre}`)}
            className="w-full flex items-center gap-2 px-2 py-1.5 rounded-lg text-xs hover:bg-slate-50 text-left"
          >
            {en.es_dir
              ? <Folder size={12} className="text-amber-400 flex-shrink-0" />
              : <FileIcon size={12} className="text-slate-300 flex-shrink-0" />}
            <span className="truncate flex-1" style={en.es_dir ? { color } : {}}>{en.nombre}</span>
          </button>
        ))}
      </div>
    </div>
  )
}

// ─── Selector de insumo: subir archivo o elegir del FTP ───────────────────
function InsumoSelector({
  titulo,
  servidor,
  rutaInicialFtp,
  accept,
  archivo,
  setArchivo,
  ruta,
  setRuta,
  detectando,
  color,
}: {
  titulo: string
  servidor: 'principal' | 'neotel17'
  rutaInicialFtp: string
  accept: string
  archivo: File | null
  setArchivo: (f: File | null) => void
  ruta: string | null
  setRuta: (r: string | null) => void
  detectando: boolean
  color: string
}) {
  const [modo, setModo] = useState<'subir' | 'ftp'>(ruta ? 'ftp' : 'subir')
  const [browserAbierto, setBrowserAbierto] = useState(false)
  const [rutaBrowser, setRutaBrowser] = useState(rutaInicialFtp)
  const [dragging, setDragging] = useState(false)
  const fileRef = useRef<HTMLInputElement>(null)

  const abrirEnCarpeta = (rutaArchivo: string) => {
    const idx = rutaArchivo.lastIndexOf('/')
    setRutaBrowser(idx > 0 ? rutaArchivo.slice(0, idx) : rutaInicialFtp)
    setBrowserAbierto(true)
  }

  // Si el padre setea la ruta (detección automática o selección manual),
  // mostrar el modo FTP y cerrar el explorador si había quedado abierto.
  useEffect(() => {
    if (ruta) {
      setModo('ftp')
      setBrowserAbierto(false)
    }
  }, [ruta])

  const limpiar = () => { setArchivo(null); setRuta(null) }

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between">
        <p className="text-xs font-semibold text-slate-500">{titulo}</p>
        <div className="flex gap-1 text-xs">
          <button
            onClick={() => setModo('subir')}
            className="px-2 py-0.5 rounded-md font-medium transition-colors"
            style={modo === 'subir' ? { backgroundColor: `${color}15`, color } : { color: '#94a3b8' }}
          >
            Subir
          </button>
          <button
            onClick={() => {
              setModo('ftp')
              if (!ruta && !detectando) { setRutaBrowser(rutaInicialFtp); setBrowserAbierto(true) }
            }}
            className="px-2 py-0.5 rounded-md font-medium transition-colors"
            style={modo === 'ftp' ? { backgroundColor: `${color}15`, color } : { color: '#94a3b8' }}
          >
            FTP
          </button>
        </div>
      </div>

      {modo === 'subir' && (
        <div
          onDrop={e => {
            e.preventDefault()
            setDragging(false)
            const f = e.dataTransfer.files?.[0]
            if (f) setArchivo(f)
          }}
          onDragOver={e => { e.preventDefault(); setDragging(true) }}
          onDragLeave={() => setDragging(false)}
          onClick={() => !archivo && fileRef.current?.click()}
          className="rounded-xl border-2 border-dashed flex items-center gap-2 px-3 py-2.5 transition-all"
          style={{
            borderColor: dragging ? color : `${color}30`,
            backgroundColor: dragging ? `${color}08` : 'transparent',
            cursor: archivo ? 'default' : 'pointer',
          }}
        >
          {archivo ? (
            <>
              <FileText size={13} style={{ color }} className="flex-shrink-0" />
              <span className="text-xs text-slate-600 flex-1 truncate">{archivo.name}</span>
              <button onClick={e => { e.stopPropagation(); limpiar() }} className="text-slate-300 hover:text-slate-500">
                <X size={12} />
              </button>
            </>
          ) : (
            <>
              <Upload size={13} className="text-slate-300 flex-shrink-0" />
              <span className="text-xs text-slate-400">{dragging ? 'Suelta aquí' : 'Arrastra o selecciona'}</span>
            </>
          )}
          <input
            ref={fileRef}
            type="file"
            accept={accept}
            className="hidden"
            onChange={e => { const f = e.target.files?.[0]; if (f) setArchivo(f) }}
          />
        </div>
      )}

      {modo === 'ftp' && (
        <div className="relative">
          {ruta ? (
            <div
              onClick={() => abrirEnCarpeta(ruta)}
              title="Ver la carpeta y elegir otro archivo"
              className="rounded-xl border border-slate-200 flex items-center gap-2 px-3 py-2.5 cursor-pointer hover:border-slate-300 transition-colors"
            >
              <FileText size={13} style={{ color }} className="flex-shrink-0" />
              <span className="text-xs text-slate-600 flex-1 truncate font-mono">{ruta}</span>
              <button onClick={e => { e.stopPropagation(); limpiar() }} className="text-slate-300 hover:text-slate-500 flex-shrink-0">
                <X size={12} />
              </button>
            </div>
          ) : detectando ? (
            <div className="w-full rounded-xl border-2 border-dashed px-3 py-2.5 text-xs text-slate-400 flex items-center gap-2" style={{ borderColor: `${color}30` }}>
              <Loader2 size={12} className="animate-spin" /> Buscando el más reciente...
            </div>
          ) : (
            <button
              onClick={() => setBrowserAbierto(true)}
              className="w-full rounded-xl border-2 border-dashed px-3 py-2.5 text-xs text-slate-400 text-left"
              style={{ borderColor: `${color}30` }}
            >
              Elegir archivo del FTP...
            </button>
          )}
          {browserAbierto && (
            <FtpBrowser
              servidor={servidor}
              rutaInicial={rutaBrowser}
              color={color}
              onCerrar={() => setBrowserAbierto(false)}
              onSeleccionar={r => { setRuta(r); setBrowserAbierto(false) }}
            />
          )}
        </div>
      )}
    </div>
  )
}

// ─── Métricas de resultado ─────────────────────────────────────────────────
const RESULT_METRICAS: { key: keyof ResultadoCargaMensual; label: string; color?: string; dimIfZero?: boolean }[] = [
  { key: 'total_entrada', label: 'Entrada' },
  { key: 'total_carga', label: 'Carga', color: '#10b981' },
  { key: 'total_sin_cruce', label: 'Sin cruce', color: '#f59e0b', dimIfZero: true },
  { key: 'total_eliminados', label: 'A eliminar', color: '#ef4444', dimIfZero: true },
  { key: 'total_no_cargados_comunas', label: 'No cargados (comuna)', color: '#8b5cf6', dimIfZero: true },
]

// ─── Card principal por tipo (PL / REFI) ──────────────────────────────────
function CargaMensualCard({ tipo, label, color }: { tipo: 'PL' | 'REFI'; label: string; color: string }) {
  const [txt, setTxt] = useState<File | null>(null)
  const [txtRuta, setTxtRuta] = useState<string | null>(null)
  const [excel, setExcel] = useState<File | null>(null)
  const [excelRuta, setExcelRuta] = useState<string | null>(null)

  const [phase, setPhase] = useState<'idle' | 'loading' | 'done'>('idle')
  const [steps, setSteps] = useState<ProgressStep[]>([])
  const [resultado, setResultado] = useState<ResultadoCargaMensual | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [detectando, setDetectando] = useState(false)
  const [errorDeteccion, setErrorDeteccion] = useState<string | null>(null)
  const esRef = useRef<EventSource | null>(null)

  const listo = (!!txt || !!txtRuta) && (!!excel || !!excelRuta)

  const detectarAutomaticamente = async () => {
    setDetectando(true)
    setErrorDeteccion(null)
    try {
      const res = await fetch(`${API}/carga-mensual/${tipo.toLowerCase()}/sugerir`, {
        headers: authHeaders(),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'No se pudo detectar')
      }
      const data = await res.json()
      if (data.txt_ruta) { setTxt(null); setTxtRuta(data.txt_ruta) }
      if (data.excel_ruta) { setExcel(null); setExcelRuta(data.excel_ruta) }
      if (!data.txt_ruta || !data.excel_ruta) {
        setErrorDeteccion('No se encontraron ambos archivos automáticamente; revisa manualmente en la pestaña FTP.')
      }
    } catch (e: any) {
      setErrorDeteccion(e.message)
    } finally {
      setDetectando(false)
    }
  }

  // Al abrir el panel, buscar solo los archivos más recientes (sin botón)
  useEffect(() => { detectarAutomaticamente() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const procesar = async () => {
    setPhase('loading')
    setResultado(null)
    setError(null)
    setSteps([{ step: 'Iniciando...', elapsed: 0 }])

    try {
      const form = new FormData()
      if (txt) form.append('txt', txt)
      if (txtRuta) form.append('txt_ruta', txtRuta)
      if (excel) form.append('excel', excel)
      if (excelRuta) form.append('excel_ruta', excelRuta)

      const res = await fetch(`${API}/carga-mensual/${tipo.toLowerCase()}/procesar`, {
        method: 'POST',
        headers: authHeaders(),
        body: form,
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'Error al procesar')
      }

      const { job_id } = await res.json()
      const es = new EventSource(`${API}/jobs/${job_id}/stream`)
      esRef.current = es

      es.onmessage = (e) => {
        if (!e.data || e.data.startsWith(':')) return
        try {
          const msg: any = JSON.parse(e.data)
          setSteps(prev => {
            const base = prev.length === 1 && prev[0].step === 'Iniciando...' ? [] : prev
            return [...base, { step: msg.step, elapsed: msg.elapsed }]
          })
          if (msg.done) {
            es.close()
            esRef.current = null
            if (msg.error) {
              setError(msg.error)
              setPhase('done')
            } else {
              setResultado(msg.result ?? null)
              setPhase('done')
            }
          }
        } catch { }
      }

      es.onerror = () => {
        es.close()
        esRef.current = null
        setPhase('done')
        setError('Error de conexión')
      }
    } catch (e: any) {
      setError(e.message)
      setPhase('done')
      setSteps([])
    }
  }

  const reset = () => {
    setPhase('idle')
    setTxt(null); setTxtRuta(null)
    setExcel(null); setExcelRuta(null)
    setResultado(null)
    setError(null)
    setSteps([])
    esRef.current?.close()
    esRef.current = null
  }

  const descargar = async (path: string, nombre: string) => {
    const res = await fetch(`${API}/descargar?path=${encodeURIComponent(path)}`, { headers: authHeaders() })
    if (!res.ok) return
    const blob = await res.blob()
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = nombre
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div
      className="rounded-2xl w-full relative"
      style={{
        background: `linear-gradient(135deg, ${color}08 0%, white 60%)`,
        border: `1px solid ${color}25`,
        boxShadow: `0 4px 24px ${color}10, 0 1px 3px rgba(0,0,0,0.06)`,
      }}
    >
      {/* Wrapper propio para el círculo decorativo: así el overflow-hidden
          no recorta el desplegable del explorador FTP, que se posiciona
          absoluto por encima del contenido de la tarjeta. */}
      <div className="absolute inset-0 rounded-2xl overflow-hidden pointer-events-none">
        <div
          className="absolute top-0 right-0 w-36 h-36 rounded-full opacity-[0.06]"
          style={{ backgroundColor: color, transform: 'translate(30%, -30%)' }}
        />
      </div>

      <div className="relative p-5 space-y-4">
        <div className="flex items-center gap-2">
          <Activity size={13} style={{ color }} />
          <p className="text-xs font-bold uppercase tracking-widest" style={{ color }}>{label}</p>
        </div>

        {phase === 'idle' && (
          <div className="space-y-3 animate-fade-in">
            {errorDeteccion && (
              <p className="text-xs text-amber-600 px-1">{errorDeteccion}</p>
            )}
            <InsumoSelector
              titulo="TXT de resoluciones (Neotel17)"
              servidor="neotel17"
              rutaInicialFtp="/DOWNLOAD/Resultante_PL"
              accept=".txt"
              archivo={txt} setArchivo={setTxt}
              ruta={txtRuta} setRuta={setTxtRuta}
              detectando={detectando}
              color={color}
            />
            <InsumoSelector
              titulo="Excel mensual (FTP principal)"
              servidor="principal"
              rutaInicialFtp="/archivos"
              accept=".xls,.xlsx"
              archivo={excel} setArchivo={setExcel}
              ruta={excelRuta} setRuta={setExcelRuta}
              detectando={detectando}
              color={color}
            />
            <button
              onClick={procesar}
              disabled={!listo}
              className="w-full py-2.5 rounded-xl text-white text-sm font-semibold disabled:opacity-40 transition-all flex items-center justify-center gap-2"
              style={{
                background: listo ? `linear-gradient(135deg, ${color}, ${color}cc)` : '#e2e8f0',
                boxShadow: listo ? `0 4px 12px ${color}30` : 'none',
                color: listo ? 'white' : '#94a3b8',
              }}
            >
              <ChevronRight size={15} /> Procesar {label}
            </button>
          </div>
        )}

        {phase === 'loading' && steps.length > 0 && (
          <div className="animate-fade-in space-y-1.5">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Procesando</span>
            </div>
            {steps.map((s, i) => (
              <div key={i} className="flex items-center gap-2">
                {i === steps.length - 1 ? (
                  <Loader2 size={11} className="animate-spin flex-shrink-0" style={{ color }} />
                ) : (
                  <div
                    className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                    style={{ backgroundColor: `${color}30`, border: `1.5px solid ${color}` }}
                  />
                )}
                <span className="text-xs flex-1 break-all text-slate-500">{s.step}</span>
              </div>
            ))}
          </div>
        )}

        {phase === 'done' && (
          <div className="animate-fade-in space-y-4">
            {error ? (
              <div
                className="rounded-xl px-3 py-2.5 text-xs"
                style={{ backgroundColor: '#fef2f2', border: '1px solid #fecaca', color: '#dc2626' }}
              >
                <span className="font-semibold">Error: </span>{error}
              </div>
            ) : resultado ? (
              <div className="space-y-3">
                <div className="grid grid-cols-2 gap-2">
                  {RESULT_METRICAS.map(m => {
                    const val = resultado[m.key] as number
                    const dim = m.dimIfZero && val === 0
                    const col = m.color || color
                    return (
                      <div
                        key={m.key as string}
                        className="rounded-xl px-3 py-2.5 flex items-center justify-between"
                        style={{
                          backgroundColor: dim ? '#f8fafc' : `${col}10`,
                          border: `1px solid ${dim ? '#e2e8f0' : `${col}20`}`,
                          opacity: dim ? 0.55 : 1,
                        }}
                      >
                        <span className="text-xs text-slate-500">{m.label}</span>
                        <span className="text-base font-bold tabular-nums" style={{ color: dim ? '#94a3b8' : col }}>
                          {val ?? '—'}
                        </span>
                      </div>
                    )
                  })}
                </div>

                <div className="border-t pt-2" style={{ borderColor: `${color}15` }} />

                <div className="flex flex-wrap gap-1.5">
                  {(resultado.archivos ?? []).map(({ nombre, path }) => (
                    <button
                      key={path}
                      onClick={() => descargar(path, nombre)}
                      title={`Descargar ${nombre}`}
                      className="inline-flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-xs font-medium transition-all hover:opacity-80 active:scale-95"
                      style={{ backgroundColor: `${color}10`, border: `1px solid ${color}30`, color }}
                    >
                      <FileText size={10} />
                      {labelArchivo(nombre)}
                      <Download size={9} className="opacity-60" />
                    </button>
                  ))}
                </div>
              </div>
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

// ─── Panel exportado: PL + REFI lado a lado ───────────────────────────────
export function CargaMensualPanel() {
  return (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
      <CargaMensualCard tipo="PL" label="Pago Liviano · Carga Mensual" color="#d97706" />
      <CargaMensualCard tipo="REFI" label="Refinanciamiento · Carga Mensual" color="#059669" />
    </div>
  )
}
