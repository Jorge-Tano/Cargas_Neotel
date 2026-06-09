'use client'

import { useState, useEffect, useCallback } from 'react'
import { RefreshCw, CheckCircle, XCircle, Clock, FolderOpen, Wifi } from 'lucide-react'
import { API } from '../lib/api'

// ─── Tipos ───────────────────────────────────────────────────────────────────

type ArchivoFTP = {
  tipo:    string
  nombre:  string
  horario: string
  fecha:   string
  mtime:   number
}

type HistorialEntry = {
  ts:        string
  resultado: 'sin_archivos' | 'procesado' | 'error'
  detalle:   string
}

type WatcherStatus = {
  activo:           boolean
  ultima_corrida:   string | null
  proxima_corrida:  string | null
  corridas_total:   number
  ultimo_resultado: 'sin_archivos' | 'procesado' | 'error' | '—'
  ultimo_error:     string | null
  archivos_en_ftp:  ArchivoFTP[]
  historial:        HistorialEntry[]
  procesados:       string[]
}

// Reconstruye la clave igual que el backend: "{TIPO}_{HORARIO}_{DDMMYYYY}"
// El backend usa datetime.now() al momento de procesar, no la fecha del archivo.
function claveArchivo(a: ArchivoFTP): string {
  const hoy  = new Date()
  const dd   = String(hoy.getDate()).padStart(2, '0')
  const mm   = String(hoy.getMonth() + 1).padStart(2, '0')
  const yyyy = hoy.getFullYear()
  return `${a.tipo}_${a.horario}_${dd}${mm}${yyyy}`
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function fmtFecha(iso: string | null) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString('es-CO', {
    day: '2-digit', month: '2-digit', year: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

const TIPO_COLORS: Record<string, string> = {
  SAV:  'bg-emerald-50 text-emerald-700 border-emerald-200',
  AV:   'bg-blue-50   text-blue-700   border-blue-200',
  REFI: 'bg-amber-50  text-amber-700  border-amber-200',
  PL:   'bg-purple-50 text-purple-700 border-purple-200',
}

const RESULTADO_CFG = {
  procesado:    { label: 'Procesado',    cls: 'bg-emerald-50 text-emerald-700', icon: <CheckCircle size={13} /> },
  error:        { label: 'Error',        cls: 'bg-red-50    text-red-700',      icon: <XCircle     size={13} /> },
  sin_archivos: { label: 'Sin archivos', cls: 'bg-slate-100 text-slate-500',    icon: <Clock       size={13} /> },
  '—':          { label: '—',            cls: 'bg-slate-100 text-slate-400',    icon: null },
}

function Badge({ resultado }: { resultado: string }) {
  const cfg = RESULTADO_CFG[resultado as keyof typeof RESULTADO_CFG] ?? RESULTADO_CFG['—']
  return (
    <span className={`inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded-full ${cfg.cls}`}>
      {cfg.icon}{cfg.label}
    </span>
  )
}

// ─── Componente principal ─────────────────────────────────────────────────────

export function WatcherPanel() {
  const [data,        setData]        = useState<WatcherStatus | null>(null)
  const [loading,     setLoading]     = useState(true)
  const [error,       setError]       = useState<string | null>(null)
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null)
  const [filtro,      setFiltro]      = useState<'todos' | 'procesado' | 'error' | 'sin_archivos'>('todos')

  const fetchStatus = useCallback(async () => {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), 5000)
    try {
      setError(null)
      const res = await fetch(`${API}/watcher/status`, { signal: controller.signal })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      setData(await res.json())
      setLastRefresh(new Date())
    } catch (e: any) {
      setError(e.message ?? 'Error de conexión')
    } finally {
      clearTimeout(timer)
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchStatus()
    const id = setInterval(fetchStatus, 30000)
    return () => clearInterval(id)
  }, [fetchStatus])

  const historialFiltrado = (data?.historial ?? []).filter(
    h => filtro === 'todos' || h.resultado === filtro
  )

  // ── Loading ──
  if (loading) return (
    <div className="flex items-center justify-center py-32">
      <div className="w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
    </div>
  )

  // ── Error ──
  if (error && !data) return (
    <div className="bg-red-50 border border-red-200 rounded-xl p-6 text-red-700 text-sm">
      No se pudo conectar al servidor: {error}
    </div>
  )

  return (
    <div className="space-y-5 max-w-4xl">

      {/* ── Cabecera ── */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${data?.activo ? 'bg-emerald-400' : 'bg-red-400'}`} />
          <span className="text-sm text-slate-500">
            {data?.activo ? 'Watcher activo' : 'Watcher inactivo'}
          </span>
          {error && (
            <span className="text-xs text-red-500 ml-2">(último fetch falló: {error})</span>
          )}
        </div>
        <div className="flex items-center gap-3">
          {lastRefresh && (
            <span className="text-xs text-slate-400">
              Actualizado {lastRefresh.toLocaleTimeString('es-CO')}
            </span>
          )}
          <button
            onClick={fetchStatus}
            className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-700 bg-white border border-slate-200 hover:border-slate-300 rounded-lg px-3 py-1.5 transition-colors"
          >
            <RefreshCw size={13} />
            Actualizar
          </button>
        </div>
      </div>

      {/* ── Métricas ── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {[
          { label: 'Última corrida',  value: fmtFecha(data?.ultima_corrida  ?? null) },
          { label: 'Próxima corrida', value: fmtFecha(data?.proxima_corrida ?? null) },
          { label: 'Corridas totales', value: String(data?.corridas_total ?? 0) },
          { label: 'Último resultado', value: data?.ultimo_resultado ?? '—', badge: true },
        ].map(({ label, value, badge }) => (
          <div key={label} className="bg-white rounded-xl border border-slate-100 px-4 py-3">
            <p className="text-xs text-slate-400 mb-1">{label}</p>
            {badge
              ? <Badge resultado={value} />
              : <p className="text-sm font-medium text-slate-700 leading-snug">{value}</p>
            }
          </div>
        ))}
      </div>

      {/* ── Error detalle ── */}
      {data?.ultimo_error && (
        <div className="bg-red-50 border border-red-200 rounded-xl px-4 py-3 text-sm text-red-700">
          <span className="font-medium">Último error: </span>{data.ultimo_error}
        </div>
      )}

      {/* ── Archivos en FTP ── */}
      <div className="bg-white rounded-xl border border-slate-100">
        <div className="flex items-center gap-2 px-5 py-3.5 border-b border-slate-100">
          <FolderOpen size={15} className="text-slate-400" />
          <span className="text-sm font-medium text-slate-700">Archivos en FTP</span>
          <span className="ml-auto text-xs text-slate-400">{data?.archivos_en_ftp.length ?? 0} archivo(s)</span>
        </div>

        {!data?.archivos_en_ftp.length ? (
          <p className="text-sm text-slate-400 px-5 py-4">No se encontraron archivos en el último scan.</p>
        ) : (
          <div className="divide-y divide-slate-50">
            {[...data.archivos_en_ftp].sort((a, b) => b.mtime - a.mtime).map((a, i) => {
              const procesado = (data.procesados ?? []).includes(claveArchivo(a))
              return (
                <div key={i} className="flex items-center gap-3 px-5 py-3 hover:bg-slate-50 transition-colors">
                  <span className={`text-xs font-semibold px-2 py-0.5 rounded border ${TIPO_COLORS[a.tipo] ?? 'bg-slate-50 text-slate-600 border-slate-200'}`}>
                    {a.tipo}
                  </span>
                  <span className="text-sm text-slate-700 flex-1 truncate" title={a.nombre}>
                    {a.nombre}
                  </span>
                  <span className="text-xs text-slate-400 whitespace-nowrap hidden sm:block">
                    {a.horario !== 'SIN_HORARIO' ? a.horario : ''}
                  </span>
                  <span className="text-xs text-slate-400 whitespace-nowrap">{a.fecha}</span>
                  {procesado
                    ? <span className="text-xs font-medium px-2 py-0.5 rounded-full bg-emerald-50 text-emerald-700 whitespace-nowrap">✓ Procesado</span>
                    : <span className="text-xs font-medium px-2 py-0.5 rounded-full bg-amber-50 text-amber-700 whitespace-nowrap">Pendiente</span>
                  }
                </div>
              )
            })}
          </div>
        )}
      </div>

      {/* ── Historial ── */}
      <div className="bg-white rounded-xl border border-slate-100">
        <div className="flex items-center gap-2 px-5 py-3.5 border-b border-slate-100">
          <Clock size={15} className="text-slate-400" />
          <span className="text-sm font-medium text-slate-700">Historial de corridas</span>
          <div className="ml-auto flex items-center gap-1">
            {(['todos', 'procesado', 'sin_archivos', 'error'] as const).map(f => (
              <button
                key={f}
                onClick={() => setFiltro(f)}
                className={`text-xs px-2.5 py-1 rounded-lg transition-colors ${
                  filtro === f
                    ? 'bg-blue-50 text-blue-700 font-medium'
                    : 'text-slate-400 hover:text-slate-600 hover:bg-slate-50'
                }`}
              >
                {f === 'todos' ? 'Todos' : f === 'procesado' ? 'Procesados' : f === 'sin_archivos' ? 'Sin archivos' : 'Errores'}
              </button>
            ))}
          </div>
        </div>

        {!historialFiltrado.length ? (
          <p className="text-sm text-slate-400 px-5 py-4">Sin registros.</p>
        ) : (
          <div className="divide-y divide-slate-50">
            {historialFiltrado.map((h, i) => (
              <div key={i} className="flex items-start gap-3 px-5 py-3 hover:bg-slate-50 transition-colors">
                <span className="text-xs text-slate-400 whitespace-nowrap pt-0.5 w-36 flex-shrink-0">
                  {fmtFecha(h.ts)}
                </span>
                <Badge resultado={h.resultado} />
                <span className="text-xs text-slate-500 flex-1 leading-relaxed">{h.detalle}</span>
              </div>
            ))}
          </div>
        )}
      </div>

    </div>
  )
}