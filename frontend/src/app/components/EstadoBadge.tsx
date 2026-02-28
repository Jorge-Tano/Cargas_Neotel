'use client'

import { CheckCircle2, XCircle } from 'lucide-react'

export type EstadoProceso = 'idle' | 'loading' | 'success' | 'error'

export function EstadoBadge({ estado }: { estado: EstadoProceso }) {
  if (estado === 'idle') return null

  const cfg = {
    loading: { icon: <span className="spinner" />,   label: 'Procesando...', cls: 'text-blue-600' },
    success: { icon: <CheckCircle2 size={14} />,     label: 'Completado',    cls: 'text-emerald-600' },
    error:   { icon: <XCircle size={14} />,          label: 'Error',         cls: 'text-red-500' },
  }[estado as 'loading' | 'success' | 'error']

  return (
    <span className={`flex items-center gap-1.5 text-sm font-medium ${cfg.cls}`}>
      {cfg.icon} {cfg.label}
    </span>
  )
}