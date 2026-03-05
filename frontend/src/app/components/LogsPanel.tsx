'use client'

import { useEffect, useState, useRef } from 'react'
import { Activity, RefreshCw, Pause, Play, ShieldCheck } from 'lucide-react'
import { getLogs, LogEntry, API } from '../lib/api'

const COLOR_POR_CASO: Record<string, string> = {
  SAV:      'bg-blue-100 text-blue-700',
  AV:       'bg-purple-100 text-purple-700',
  REFI:     'bg-emerald-100 text-emerald-700',
  PL:       'bg-amber-100 text-amber-700',
  PERDIDAS: 'bg-red-100 text-red-700',
}

const INTERVALOS = [10, 30, 60]

interface AuditoriaEntry {
  id: number
  fecha: string
  usuario: string
  accion: string
  detalle: string
}

async function getAuditoria(limit = 100): Promise<AuditoriaEntry[]> {
  const token = typeof window !== 'undefined' ? sessionStorage.getItem('auth_token') : null
  const res = await fetch(`${API}/auditoria?limit=${limit}`, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  })
  if (!res.ok) return []
  return res.json()
}

type Tab = 'procesos' | 'auditoria'

export function LogsPanel() {
  const [tab, setTab] = useState<Tab>('procesos')
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [loadingLogs, setLoadingLogs] = useState(true)
  const [auditoria, setAuditoria] = useState<AuditoriaEntry[]>([])
  const [loadingAudit, setLoadingAudit] = useState(true)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [intervalo, setIntervalo] = useState(30)
  const [countdown, setCountdown] = useState(30)
  const intervalRef = useRef<NodeJS.Timeout | null>(null)
  const countdownRef = useRef<NodeJS.Timeout | null>(null)

  const cargarLogs = async () => {
    setLoadingLogs(true)
    const data = await getLogs()
    setLogs(data)
    setLoadingLogs(false)
  }

  const cargarAuditoria = async () => {
    setLoadingAudit(true)
    const data = await getAuditoria()
    setAuditoria(data)
    setLoadingAudit(false)
  }

  const cargarTodo = () => {
    cargarLogs()
    cargarAuditoria()
    setCountdown(intervalo)
  }

  useEffect(() => { cargarTodo() }, [])

  useEffect(() => {
    if (!autoRefresh) {
      if (intervalRef.current) clearInterval(intervalRef.current)
      if (countdownRef.current) clearInterval(countdownRef.current)
      return
    }
    intervalRef.current = setInterval(cargarTodo, intervalo * 1000)
    countdownRef.current = setInterval(() => {
      setCountdown(prev => (prev <= 1 ? intervalo : prev - 1))
    }, 1000)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
      if (countdownRef.current) clearInterval(countdownRef.current)
    }
  }, [autoRefresh, intervalo])

  const toggleAutoRefresh = () => {
    setAutoRefresh(prev => !prev)
    if (!autoRefresh) setCountdown(intervalo)
  }

  const cambiarIntervalo = (seg: number) => {
    setIntervalo(seg)
    setCountdown(seg)
  }

  const loading = tab === 'procesos' ? loadingLogs : loadingAudit

  return (
    <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
      <div className="flex items-center justify-between px-5 py-4 border-b border-slate-100">
        <div className="flex items-center gap-1 rounded-lg border border-slate-200 p-0.5">
          <button
            onClick={() => setTab('procesos')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === 'procesos' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            <Activity size={12} /> Procesos
          </button>
          <button
            onClick={() => setTab('auditoria')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === 'auditoria' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            <ShieldCheck size={12} /> Auditoría
          </button>
        </div>

        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1 rounded-lg border border-slate-200 p-0.5">
            {INTERVALOS.map(seg => (
              <button
                key={seg}
                onClick={() => cambiarIntervalo(seg)}
                className={`px-2 py-1 rounded-md text-xs font-medium transition-colors ${
                  intervalo === seg ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-700'
                }`}
              >
                {seg}s
              </button>
            ))}
          </div>
          <button
            onClick={toggleAutoRefresh}
            className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors ${
              autoRefresh ? 'bg-emerald-50 text-emerald-700 hover:bg-emerald-100' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
            }`}
          >
            {autoRefresh ? <Pause size={12} /> : <Play size={12} />}
            {autoRefresh ? `${countdown}s` : 'Pausado'}
          </button>
          <button
            onClick={cargarTodo}
            disabled={loading}
            className="p-1.5 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors disabled:opacity-40"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          </button>
        </div>
      </div>

      {tab === 'procesos' && (
        <div className="overflow-x-auto">
          {loadingLogs && logs.length === 0 ? (
            <div className="flex justify-center py-10">
              <span className="spinner !border-slate-300 !border-t-slate-600" />
            </div>
          ) : logs.length === 0 ? (
            <p className="text-center text-slate-400 text-sm py-10">No hay registros aún</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100 bg-slate-50">
                  {['Caso', 'Usuario', 'Fecha', 'Entrada', 'Repetidos', 'Bloqueados', 'Carga', 'Archivo'].map(h => (
                    <th key={h} className="px-4 py-2.5 text-left text-xs font-semibold text-slate-500 uppercase tracking-wide whitespace-nowrap">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {logs.map((log, i) => (
                  <tr key={log.id} className={`border-b border-slate-50 hover:bg-slate-50 transition-colors ${i % 2 === 0 ? '' : 'bg-slate-50/50'}`}>
                    <td className="px-4 py-2.5">
                      <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${COLOR_POR_CASO[log.tipo_caso] || 'bg-slate-100 text-slate-600'}`}>
                        {log.tipo_caso}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-xs text-slate-600">
                      {(log as any).usuario || <span className="text-slate-300">—</span>}
                    </td>
                    <td className="px-4 py-2.5 text-slate-500 font-mono text-xs whitespace-nowrap">
                      {new Date(log.fecha_proceso).toLocaleString('es-CL')}
                    </td>
                    <td className="px-4 py-2.5 text-slate-700 font-medium">{log.total_entrada}</td>
                    <td className="px-4 py-2.5 text-amber-600">{log.total_repetidos}</td>
                    <td className="px-4 py-2.5 text-red-500">{log.total_bloqueados}</td>
                    <td className="px-4 py-2.5 text-emerald-600 font-semibold">{log.total_carga}</td>
                    <td className="px-4 py-2.5 text-slate-400 font-mono text-xs max-w-[200px] truncate">
                      {log.archivo_origen}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      {tab === 'auditoria' && (
        <div className="overflow-x-auto">
          {loadingAudit && auditoria.length === 0 ? (
            <div className="flex justify-center py-10">
              <span className="spinner !border-slate-300 !border-t-slate-600" />
            </div>
          ) : auditoria.length === 0 ? (
            <p className="text-center text-slate-400 text-sm py-10">No hay registros de auditoría aún</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100 bg-slate-50">
                  {['Usuario', 'Acción', 'Detalle', 'Fecha'].map(h => (
                    <th key={h} className="px-4 py-2.5 text-left text-xs font-semibold text-slate-500 uppercase tracking-wide whitespace-nowrap">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {auditoria.map((a, i) => (
                  <tr key={a.id} className={`border-b border-slate-50 hover:bg-slate-50 transition-colors ${i % 2 === 0 ? '' : 'bg-slate-50/50'}`}>
                    <td className="px-4 py-2.5 text-xs text-slate-700 font-medium whitespace-nowrap">
                      {a.usuario || <span className="text-slate-300">—</span>}
                    </td>
                    <td className="px-4 py-2.5">
                      <span className="rounded-full px-2 py-0.5 text-xs font-medium bg-slate-100 text-slate-600">
                        {a.accion}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-slate-500 text-xs max-w-[320px] truncate">
                      {a.detalle}
                    </td>
                    <td className="px-4 py-2.5 text-slate-400 font-mono text-xs whitespace-nowrap">
                      {new Date(a.fecha).toLocaleString('es-CL')}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}