'use client'

import { useState, useMemo } from 'react'
import { Wrench, Loader2 } from 'lucide-react'
import { BaseCampanaPanel } from './BaseCampanaPanel'
import { useAuth } from '../hooks/useAuth'
import { verificarCargaMensual } from '../lib/api'

// Agregar acá cada nuevo tipo de campaña que sume soporte de Crear/Actualizar
// Base (el backend ya es genérico por `tipo`, ver app/services/campanas_mensuales.py
// — solo falta habilitar el ECRM correspondiente ahí y en Neotel). El
// permiso correspondiente (bases_pl/bases_refi) se declara en
// app.core.permisos.ITEMS_BASES (backend) — agregar ahí también.
const TIPOS_BASE: { value: 'pl' | 'refi'; label: string; color: string; permiso: string }[] = [
  { value: 'pl', label: 'PL', color: '#7c3aed', permiso: 'bases_pl' },
  { value: 'refi', label: 'REFI', color: '#059669', permiso: 'bases_refi' },
]

export function BasesView({ permisos }: { permisos?: Record<string, boolean> }) {
  const { user } = useAuth()
  const tiposVisibles = useMemo(
    () => TIPOS_BASE.filter(t => permisos?.[t.permiso] ?? true),
    [permisos]
  )
  const [activo, setActivo] = useState<'pl' | 'refi'>(tiposVisibles[0]?.value ?? 'pl')
  const tipo = tiposVisibles.find(t => t.value === activo) ?? tiposVisibles[0]

  const [verificando, setVerificando] = useState(false)
  const [resultado, setResultado] = useState<string | null>(null)

  const probarVerificacion = async () => {
    if (!tipo) return
    if (!window.confirm(
      `Esto va a revisar la base mensual más reciente de ${tipo.label} y, si algo no coincide con la plantilla ` +
      `correcta, la va a CORREGIR de verdad en Neotel (ignorando el día 15). ¿Continuar?`
    )) return

    setVerificando(true)
    setResultado(null)
    try {
      const res = await verificarCargaMensual(tipo.value, true)
      const r = res[tipo.value.toUpperCase()]
      setResultado(r?.error ? `Error: ${r.error}` : (r?.mensaje ?? 'Sin respuesta'))
    } catch (e: any) {
      setResultado(`Error: ${e.message}`)
    } finally {
      setVerificando(false)
    }
  }

  if (!tipo) return <p className="text-sm text-slate-400">No tienes acceso a ningún tipo de base.</p>

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between gap-3">
        <div className="flex gap-2">
          {tiposVisibles.map(t => (
            <button
              key={t.value}
              onClick={() => { setActivo(t.value); setResultado(null) }}
              className="px-4 py-2 rounded-xl text-sm font-semibold transition-all"
              style={
                activo === t.value
                  ? { backgroundColor: `${t.color}15`, color: t.color, border: `1px solid ${t.color}40` }
                  : { color: '#94a3b8', border: '1px solid transparent' }
              }
            >
              {t.label}
            </button>
          ))}
        </div>

        {user?.rol === 'admin' && (
          <button
            onClick={probarVerificacion}
            disabled={verificando}
            title="Prueba manual: revisa y corrige ya la base mensual de este tipo, sin esperar al día 15"
            className="flex items-center gap-1.5 text-xs font-medium text-slate-500 hover:text-slate-700 disabled:opacity-50 bg-slate-50 hover:bg-slate-100 rounded-lg px-3 py-2 border border-slate-200"
          >
            {verificando ? <Loader2 size={13} className="animate-spin" /> : <Wrench size={13} />}
            Probar verificación mensual
          </button>
        )}
      </div>

      {resultado && (
        <div className="text-xs text-slate-500 bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
          {resultado}
        </div>
      )}

      <BaseCampanaPanel key={tipo.value} tipo={tipo.value} label={`Bases ${tipo.label}`} color={tipo.color} />
    </div>
  )
}
