'use client'

import { useEffect, useRef, useState } from 'react'
import { RefreshCw, Upload, Users } from 'lucide-react'
import { EstadoBadge, EstadoProceso } from './EstadoBadge'
import { API } from '../lib/api'
import { getToken } from '../hooks/useAuth'

function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const token = getToken()
  return { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...extra }
}

export function ListaNegraCard() {
  const [estado, setEstado] = useState<EstadoProceso>('idle')
  const [resultado, setResultado] = useState<any>(null)
  const [totalRuts, setTotalRuts] = useState<number | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  const cargarTotal = () => {
    fetch(`${API}/lista-negra/total`, { headers: authHeaders() })
      .then(r => r.json())
      .then(d => setTotalRuts(d.personas ?? null))
      .catch(() => {})
  }

  useEffect(() => { cargarTotal() }, [])

  const handleActualizar = async (file?: File) => {
    setEstado('loading')
    try {
      let res
      if (file) {
        const form = new FormData()
        form.append('file', file)
        const r = await fetch(`${API}/lista-negra/actualizar`, { method: 'POST', headers: authHeaders(), body: form })
        if (!r.ok) throw new Error((await r.json()).detail || await r.text())
        res = await r.json()
      } else {
        const r = await fetch(`${API}/lista-negra/actualizar`, { method: 'POST', headers: authHeaders() })
        if (!r.ok) throw new Error((await r.json()).detail || await r.text())
        res = await r.json()
      }
      setResultado(res)
      setEstado('success')
      cargarTotal()
    } catch (e: any) {
      setResultado({ error: e.message })
      setEstado('error')
    }
  }

  return (
    <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-4" style={{ borderLeft: '4px solid #64748b' }}>
        <div>
          <h3 className="font-semibold text-slate-800">Blacklist Gerencial</h3>
          <p className="text-xs text-slate-400 mt-0.5">Sincronización con PostgreSQL</p>
        </div>
        <EstadoBadge estado={estado} />
      </div>

      {/* Contador RUTs */}
      <div className="px-5 pt-4 pb-2">
        <div className="flex items-center gap-3 rounded-xl bg-slate-50 px-4 py-3">
          <Users size={16} className="text-slate-400" />
          <div>
            <p className="text-xs text-slate-400">Personas bloqueadas</p>
            <p className="text-lg font-bold text-slate-800">
              {totalRuts === null ? '—' : totalRuts.toLocaleString('es-CL')}
            </p>
          </div>
        </div>
      </div>

      {/* Acciones */}
      <div className="px-5 py-4 border-t border-slate-100 space-y-2">
        <button
          onClick={() => handleActualizar()}
          disabled={estado === 'loading'}
          className="flex items-center gap-2 rounded-xl bg-slate-800 px-4 py-2.5 text-sm font-medium text-white hover:bg-slate-700 w-full justify-center transition-colors disabled:opacity-60"
        >
          {estado === 'loading' ? <span className="spinner" /> : <RefreshCw size={15} />}
          Actualizar
        </button>

        <input
          ref={fileRef}
          type="file"
          accept=".xls,.xlsx"
          className="hidden"
          onChange={e => { const f = e.target.files?.[0]; if (f) handleActualizar(f) }}
        />
      </div>

      {/* Resultado */}
      {resultado && !resultado.error && estado === 'success' && (
        <div className="px-5 pb-5 space-y-3 animate-fade-in">
          <div className="grid grid-cols-3 gap-2">
            {[
              { label: 'Insertados',   value: resultado.insertados  ?? 0, color: 'text-emerald-600' },
              { label: 'Actualizados', value: resultado.actualizados ?? 0, color: 'text-blue-600'    },
              { label: 'Eliminados',   value: resultado.eliminados  ?? 0, color: 'text-red-500'     },
            ].map(({ label, value, color }) => (
              <div key={label} className="text-center rounded-xl bg-slate-50 py-3">
                <p className={`text-xl font-bold ${color}`}>{value}</p>
                <p className="text-xs text-slate-400 mt-0.5">{label}</p>
              </div>
            ))}
          </div>
          <p className="text-xs text-center text-slate-400">
            Total activos: <span className="font-semibold text-slate-600">{resultado.total_activos ?? totalRuts ?? 0}</span> personas
          </p>
        </div>
      )}

      {resultado?.error && (
        <div className="px-5 pb-5">
          <p className="text-sm text-red-600 bg-red-50 rounded-xl px-3 py-2">{resultado.error}</p>
        </div>
      )}
    </div>
  )
}