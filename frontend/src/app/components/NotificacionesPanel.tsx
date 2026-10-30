'use client'

import { useEffect, useState } from 'react'
import { Send, ChevronRight, CheckCircle2, AlertTriangle, Loader2 } from 'lucide-react'
import { API } from '../lib/api'
import { getToken } from '../hooks/useAuth'

function authHeaders(): Record<string, string> {
  const token = getToken()
  return { ...(token ? { Authorization: `Bearer ${token}` } : {}), 'Content-Type': 'application/json' }
}

interface Categoria {
  clave: string
  nombre: string
  descripcion: string
  privada: boolean
}

interface Estado {
  telegram_configurado: boolean
  categorias: Categoria[]
}

// Avisos que llegan SOLO al bot de Telegram del responsable en vez de al
// grupo de Teams (donde los ven todos). El resumen "BDD Cargada
// Automáticamente" de Registros Leakage no se puede mover: es para el grupo.
export function NotificacionesPanel() {
  const [abierto, setAbierto] = useState(false)
  const [estado, setEstado] = useState<Estado | null>(null)
  const [seleccion, setSeleccion] = useState<string[]>([])
  const [guardando, setGuardando] = useState(false)
  const [probando, setProbando] = useState(false)
  const [mensaje, setMensaje] = useState<{ ok: boolean; texto: string } | null>(null)

  const cargar = async () => {
    try {
      const res = await fetch(`${API}/config/notificaciones`, { headers: authHeaders() })
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || 'Error al leer la configuración')
      const data: Estado = await res.json()
      setEstado(data)
      setSeleccion(data.categorias.filter(c => c.privada).map(c => c.clave))
    } catch (e: any) {
      setMensaje({ ok: false, texto: e.message })
    }
  }

  useEffect(() => { if (abierto && !estado) cargar() }, [abierto]) // eslint-disable-line react-hooks/exhaustive-deps

  const guardadas = estado?.categorias.filter(c => c.privada).map(c => c.clave) ?? []
  const sinCambios = guardadas.length === seleccion.length && guardadas.every(c => seleccion.includes(c))

  const alternar = (clave: string) =>
    setSeleccion(s => (s.includes(clave) ? s.filter(c => c !== clave) : [...s, clave]))

  const guardar = async () => {
    setGuardando(true)
    setMensaje(null)
    try {
      const res = await fetch(`${API}/config/notificaciones`, {
        method: 'PUT', headers: authHeaders(), body: JSON.stringify({ privadas: seleccion }),
      })
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || 'Error al guardar')
      const data: Estado = await res.json()
      setEstado(data)
      setSeleccion(data.categorias.filter(c => c.privada).map(c => c.clave))
      setMensaje({ ok: true, texto: 'Guardado' })
    } catch (e: any) {
      setMensaje({ ok: false, texto: e.message })
    } finally {
      setGuardando(false)
    }
  }

  const probar = async () => {
    setProbando(true)
    setMensaje(null)
    try {
      const res = await fetch(`${API}/config/notificaciones/prueba`, { method: 'POST', headers: authHeaders() })
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || 'No se pudo enviar')
      setMensaje({ ok: true, texto: 'Mensaje de prueba enviado: revisa tu Telegram' })
    } catch (e: any) {
      setMensaje({ ok: false, texto: e.message })
    } finally {
      setProbando(false)
    }
  }

  return (
    <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
      <button onClick={() => setAbierto(a => !a)}
        className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50 transition-colors">
        <div className="flex items-center gap-2">
          <div className="w-6 h-6 rounded-lg bg-sky-50 flex items-center justify-center flex-shrink-0">
            <Send size={13} className="text-sky-500" />
          </div>
          <div className="text-left">
            <p className="text-xs font-semibold text-slate-800 leading-tight">Notificaciones privadas (Telegram)</p>
            <p className="text-xs text-slate-400">Avisos que te llegan solo a ti y no al grupo de Teams</p>
          </div>
        </div>
        <ChevronRight size={13} className={`text-slate-400 transition-transform ${abierto ? 'rotate-90' : ''}`} />
      </button>

      {abierto && (
        <div className="border-t border-slate-100 px-4 py-3 space-y-3 animate-fade-in">
          {!estado ? (
            <div className="flex items-center gap-2 text-xs text-slate-400"><Loader2 size={12} className="animate-spin" /> Cargando…</div>
          ) : (
            <>
              {!estado.telegram_configurado && (
                <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
                  <AlertTriangle size={13} className="flex-shrink-0 mt-0.5" />
                  <span>Falta configurar el bot en el servidor: <code>TELEGRAM_BOT_TOKEN</code> y <code>TELEGRAM_CHAT_ID</code> en el <code>.env</code> del backend. Mientras tanto todos los avisos siguen yendo a Teams.</span>
                </div>
              )}

              <div className="space-y-1.5">
                {estado.categorias.map(c => (
                  <label key={c.clave} className="flex items-start gap-2.5 rounded-lg border border-slate-200 px-3 py-2 cursor-pointer hover:bg-slate-50">
                    <input type="checkbox" className="mt-0.5" checked={seleccion.includes(c.clave)} onChange={() => alternar(c.clave)} />
                    <span>
                      <span className="block text-xs font-semibold text-slate-700">{c.nombre}</span>
                      <span className="block text-xs text-slate-400">{c.descripcion}</span>
                    </span>
                  </label>
                ))}
              </div>
              <p className="text-xs text-slate-400">
                Lo marcado llega solo a tu Telegram y deja de publicarse en Teams. El resumen "BDD Cargada Automáticamente" del grupo Registros Leakage sigue siempre en Teams.
              </p>

              <div className="flex items-center gap-2 pt-1 border-t border-slate-100">
                <button onClick={guardar} disabled={guardando || sinCambios}
                  className="flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50 bg-sky-600">
                  {guardando ? <Loader2 size={11} className="animate-spin" /> : <CheckCircle2 size={11} />} Guardar
                </button>
                <button onClick={probar} disabled={probando || !estado.telegram_configurado}
                  className="flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-medium text-sky-700 bg-sky-50 border border-sky-200 disabled:opacity-50">
                  {probando ? <Loader2 size={11} className="animate-spin" /> : <Send size={11} />} Enviar prueba
                </button>
                {mensaje && (
                  <span className={`text-xs ${mensaje.ok ? 'text-emerald-600' : 'text-red-500'}`}>{mensaje.texto}</span>
                )}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
