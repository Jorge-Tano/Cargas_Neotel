'use client'

import { useEffect, useState } from 'react'
import { Activity, RefreshCw } from 'lucide-react'
import { getLogs, LogEntry } from '../lib/api'

const COLOR_POR_CASO: Record<string, string> = {
  SAV:      'bg-blue-100 text-blue-700',
  AV:       'bg-purple-100 text-purple-700',
  REFI:     'bg-emerald-100 text-emerald-700',
  PL:       'bg-amber-100 text-amber-700',
  PERDIDAS: 'bg-red-100 text-red-700',
}

export function LogsPanel() {
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [loading, setLoading] = useState(true)

  const cargar = () => {
    setLoading(true)
    getLogs().then(data => { setLogs(data); setLoading(false) })
  }

  useEffect(() => { cargar() }, [])

  return (
    <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
      <div className="flex items-center justify-between px-5 py-4 border-b border-slate-100">
        <div className="flex items-center gap-2">
          <Activity size={16} className="text-slate-400" />
          <h3 className="font-semibold text-slate-800">Historial de procesos</h3>
        </div>
        <button
          onClick={cargar}
          className="p-1.5 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors"
        >
          <RefreshCw size={14} />
        </button>
      </div>

      <div className="overflow-x-auto">
        {loading ? (
          <div className="flex justify-center py-10">
            <span className="spinner !border-slate-300 !border-t-slate-600" />
          </div>
        ) : logs.length === 0 ? (
          <p className="text-center text-slate-400 text-sm py-10">No hay registros aún</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-100 bg-slate-50">
                {['Caso', 'Fecha', 'Entrada', 'Repetidos', 'Bloqueados', 'Carga', 'Archivo'].map(h => (
                  <th key={h} className="px-4 py-2.5 text-left text-xs font-semibold text-slate-500 uppercase tracking-wide">
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
    </div>
  )
}