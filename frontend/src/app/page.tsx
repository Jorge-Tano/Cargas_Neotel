'use client'

import { useState, useEffect } from 'react'
import { Play, Activity, Database, Settings, ChevronRight, LogOut } from 'lucide-react'
import { CasoCard } from './components/CasoCard'
import { ListaNegraCard } from './components/ListaNegraCard'
import { LogsPanel } from './components/LogsPanel'
import { ConfigPanel } from './components/ConfigPanel'
import { CASOS, CasoKey, API } from './lib/api'
import { useAuth, logout } from './hooks/useAuth'

type Vista = 'procesar' | 'historial' | 'lista-negra' | 'configuracion'

const NAV_ITEMS: { key: Vista; icon: React.ReactNode; label: string }[] = [
  { key: 'procesar',      icon: <Play size={15} />,     label: 'Procesar' },
  { key: 'historial',     icon: <Activity size={15} />, label: 'Historial' },
  { key: 'lista-negra',   icon: <Database size={15} />, label: 'Lista Negra' },
  { key: 'configuracion', icon: <Settings size={15} />, label: 'Configuracion' },
]

const TITULOS: Record<Vista, string> = {
  'procesar':      'Panel de cargas',
  'historial':     'Historial de procesos',
  'lista-negra':   'Lista Negra',
  'configuracion': 'Configuracion',
}

type ConexionEstado = 'conectado' | 'desconectado' | 'verificando'

function useBackendPing(intervalMs = 30000) {
  const [estado, setEstado] = useState<ConexionEstado>('verificando')

  const ping = async () => {
    try {
      const res = await fetch(`${API}/health`, { signal: AbortSignal.timeout(4000) })
      setEstado(res.ok ? 'conectado' : 'desconectado')
    } catch {
      setEstado('desconectado')
    }
  }

  useEffect(() => {
    ping()
    const id = setInterval(ping, intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])

  return estado
}

const CONEXION_CFG: Record<ConexionEstado, { color: string; label: string; dot: string }> = {
  conectado:    { color: 'text-slate-500', label: 'Conectado',    dot: 'bg-emerald-400' },
  desconectado: { color: 'text-red-500',   label: 'Sin conexion', dot: 'bg-red-400' },
  verificando:  { color: 'text-slate-400', label: 'Verificando',  dot: 'bg-amber-400 animate-pulse' },
}

export default function Home() {
  const { user, loading } = useAuth()
  const [vista, setVista] = useState<Vista>('procesar')

  useEffect(() => {
    const saved = sessionStorage.getItem('neotel_tab') as Vista
    if (saved) setVista(saved)
  }, [])

  const cambiarVista = (v: Vista) => {
    setVista(v)
    sessionStorage.setItem('neotel_tab', v)
  }
  const conexion = useBackendPing(30000)
  const cfg = CONEXION_CFG[conexion]

  const hoy = new Date().toLocaleDateString('es-CL', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
  })

  const initials = user?.nombre
    ? user.nombre.split(' ').map((n: string) => n[0]).join('').slice(0, 2).toUpperCase()
    : '?'

  if (loading) return (
    <div className="min-h-screen bg-slate-50 flex items-center justify-center">
      <div className="w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
    </div>
  )

  if (!user) return null

  return (
    <div className="min-h-screen bg-slate-50">
      <aside className="fixed left-0 top-0 h-full w-56 bg-white border-r border-slate-200 z-10 flex flex-col">
        {/* Logo */}
        <div className="px-5 py-5 border-b border-slate-100">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ backgroundColor: '#0c90e6' }}>
              <Database size={16} className="text-white" />
            </div>
            <div>
              <p className="font-bold text-slate-800 text-sm leading-tight">Neotel</p>
              <p className="text-xs text-slate-400">Gestion de Cargas</p>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-3 py-4 space-y-1">
          {NAV_ITEMS.map(({ key, icon, label }) => (
            <button key={key} onClick={() => cambiarVista(key)}
              className={`w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition-colors ${
                vista === key ? 'bg-blue-50 text-blue-700 font-medium' : 'text-slate-500 hover:text-slate-700 hover:bg-slate-50'
              }`}>
              {icon}{label}
              {vista === key && <ChevronRight size={13} className="ml-auto" />}
            </button>
          ))}
        </nav>

        {/* Footer — solo conexión */}
        <div className="px-4 py-4 border-t border-slate-100">
          <p className="text-xs text-slate-400">Backend: localhost:8000</p>
          <div className="flex items-center gap-1.5 mt-0.5">
            <div className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${cfg.dot}`} />
            <p className={`text-xs ${cfg.color}`}>{cfg.label}</p>
          </div>
        </div>
      </aside>

      <div className="ml-56 flex flex-col min-h-screen">
        {/* Topbar — card flotante con avatar grande */}
        <header className="sticky top-4 z-10 h-16 flex items-center justify-end px-8 pointer-events-none">
          <div className="pointer-events-auto flex items-center gap-3 bg-white rounded-2xl shadow-md border border-slate-100 px-4 py-2.5">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-400 to-blue-600 flex items-center justify-center flex-shrink-0 shadow-sm shadow-blue-200">
              <span className="text-white font-bold text-sm">{initials}</span>
            </div>
            <div>
              <p className="font-semibold text-slate-800 text-sm leading-tight">{user.nombre}</p>
              <p className="text-xs text-slate-400 leading-tight">{user.usuario}</p>
            </div>
            <button
              onClick={logout}
              className="ml-2 flex items-center gap-1.5 text-sm text-slate-400 hover:text-red-500 transition-colors bg-slate-50 hover:bg-red-50 rounded-lg px-3 py-1.5 font-medium border border-slate-200 hover:border-red-200"
            >
              <LogOut size={14} />
              Salir
            </button>
          </div>
        </header>

        {/* Contenido */}
        <main className="flex-1 px-8 pb-8 -mt-2">
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
    </div>
  )
}