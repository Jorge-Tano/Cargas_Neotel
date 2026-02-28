'use client'

import { useState } from 'react'
import { Play, Activity, Database, Settings, ChevronRight } from 'lucide-react'
import { CasoCard } from './components/CasoCard'
import { ListaNegraCard } from './components/ListaNegraCard'
import { LogsPanel } from './components/LogsPanel'
import { ConfigPanel } from './components/ConfigPanel'
import { CASOS, CasoKey } from './lib/api'

type Vista = 'procesar' | 'historial' | 'lista-negra' | 'configuracion'

const NAV_ITEMS: { key: Vista; icon: React.ReactNode; label: string }[] = [
  { key: 'procesar',      icon: <Play size={15} />,     label: 'Procesar' },
  { key: 'historial',     icon: <Activity size={15} />, label: 'Historial' },
  { key: 'lista-negra',   icon: <Database size={15} />, label: 'Lista Negra' },
  { key: 'configuracion', icon: <Settings size={15} />, label: 'Configuración' },
]

const TITULOS: Record<Vista, string> = {
  'procesar':      'Panel de cargas',
  'historial':     'Historial de procesos',
  'lista-negra':   'Lista Negra',
  'configuracion': 'Configuración',
}

export default function Home() {
  const [vista, setVista] = useState<Vista>('procesar')
  const hoy = new Date().toLocaleDateString('es-CL', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
  })

  return (
    <div className="min-h-screen bg-slate-50">
      <aside className="fixed left-0 top-0 h-full w-56 bg-white border-r border-slate-200 z-10 flex flex-col">
        <div className="px-5 py-5 border-b border-slate-100">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ backgroundColor: '#0c90e6' }}>
              <Database size={16} className="text-white" />
            </div>
            <div>
              <p className="font-bold text-slate-800 text-sm leading-tight">Neotel</p>
              <p className="text-xs text-slate-400">Gestión de Cargas</p>
            </div>
          </div>
        </div>
        <nav className="flex-1 px-3 py-4 space-y-1">
          {NAV_ITEMS.map(({ key, icon, label }) => (
            <button key={key} onClick={() => setVista(key)}
              className={`w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition-colors ${
                vista === key ? 'bg-blue-50 text-blue-700 font-medium' : 'text-slate-500 hover:text-slate-700 hover:bg-slate-50'
              }`}>
              {icon}{label}
              {vista === key && <ChevronRight size={13} className="ml-auto" />}
            </button>
          ))}
        </nav>
        <div className="px-4 py-4 border-t border-slate-100">
          <p className="text-xs text-slate-400">Backend: localhost:8000</p>
          <div className="flex items-center gap-1.5 mt-1">
            <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
            <p className="text-xs text-slate-500">Conectado</p>
          </div>
        </div>
      </aside>

      <main className="ml-56 p-8">
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-slate-800">{TITULOS[vista]}</h1>
          <p className="text-slate-400 text-sm mt-1 capitalize">{hoy}</p>
        </div>

        {vista === 'procesar' && (
          <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
            {(Object.keys(CASOS) as CasoKey[]).map(k => <CasoCard key={k} casoKey={k} />)}
          </div>
        )}
        {vista === 'historial' && <LogsPanel />}
        {vista === 'lista-negra' && <div className="max-w-md"><ListaNegraCard /></div>}
        {vista === 'configuracion' && <ConfigPanel />}
      </main>
    </div>
  )
}