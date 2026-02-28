'use client'

import { useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { EstadoBadge, EstadoProceso } from './EstadoBadge'
import { actualizarListaNegra } from '../lib/api'

export function ListaNegraCard() {
  const [estado, setEstado] = useState<EstadoProceso>('idle')
  const [resultado, setResultado] = useState<any>(null)

  const handleActualizar = async () => {
    setEstado('loading')
    try {
      const res = await actualizarListaNegra()
      setResultado(res)
      setEstado('success')
    } catch (e: any) {
      setResultado({ error: e.message })
      setEstado('error')
    }
  }

  return (
    <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
      <div className="flex items-center justify-between px-5 py-4" style={{ borderLeft: '4px solid #64748b' }}>
        <div>
          <h3 className="font-semibold text-slate-800">Lista Negra</h3>
          <p className="text-xs text-slate-400 mt-0.5">Sincronización con PostgreSQL</p>
        </div>
        <EstadoBadge estado={estado} />
      </div>

      <div className="px-5 py-4 border-t border-slate-100 space-y-4">
        <button
          onClick={handleActualizar}
          disabled={estado === 'loading'}
          className="flex items-center gap-2 rounded-xl bg-slate-800 px-4 py-2.5 text-sm font-medium text-white hover:bg-slate-700 transition-colors disabled:opacity-60"
        >
          {estado === 'loading' ? <span className="spinner" /> : <RefreshCw size={15} />}
          Actualizar ahora
        </button>

        {resultado && !resultado.error && estado === 'success' && (
          <div className="grid grid-cols-3 gap-3 animate-fade-in">
            {[
              { label: 'Insertados',  value: resultado.insertados?.length ?? 0,  color: 'text-emerald-600' },
              { label: 'Eliminados',  value: resultado.eliminados?.length ?? 0,  color: 'text-red-500' },
              { label: 'Sin cambios', value: resultado.sin_cambios ?? 0,          color: 'text-slate-500' },
            ].map(({ label, value, color }) => (
              <div key={label} className="text-center rounded-xl bg-slate-50 py-3">
                <p className={`text-xl font-bold ${color}`}>{value}</p>
                <p className="text-xs text-slate-400 mt-0.5">{label}</p>
              </div>
            ))}
          </div>
        )}

        {resultado?.error && (
          <p className="text-sm text-red-600">{resultado.error}</p>
        )}
      </div>
    </div>
  )
}