'use client'

import { useState, useEffect } from 'react'
import { ShieldCheck, Loader2, CheckCircle2, ChevronDown, Search } from 'lucide-react'
import { API } from '../lib/api'
import { getToken } from '../hooks/useAuth'

function authHeaders(): Record<string, string> {
  const token = getToken()
  return { ...(token ? { Authorization: `Bearer ${token}` } : {}), 'Content-Type': 'application/json' }
}

const ITEMS_NAV: { key: string; label: string }[] = [
  { key: 'procesar',      label: 'Procesar' },
  { key: 'carga-mensual', label: 'Carga Mensual PL/REFI' },
  { key: 'bases',         label: 'Bases' },
  { key: 'historial',     label: 'Historial' },
  { key: 'lista-negra',   label: 'Lista Negra' },
  { key: 'repetidos',     label: 'Repetidos' },
  { key: 'configuracion', label: 'Configuración' },
  { key: 'watcher',       label: 'FTP Watcher' },
]

const ITEMS_CASOS: { key: string; label: string }[] = [
  { key: 'SAV',         label: 'SAV' },
  { key: 'AV',          label: 'AV' },
  { key: 'REFI',        label: 'REFI' },
  { key: 'PL',          label: 'Pago Liviano' },
  { key: 'MKT',         label: 'MKT' },
  { key: 'CARRITO',     label: 'Carrito Abandonado' },
  { key: 'PERDIDAS',    label: 'Llamadas Perdidas' },
  { key: 'AMALIA',      label: 'Líder Amalia' },
  { key: 'OP_PERDIDAS', label: 'Op. Pago (Perd.)' },
  { key: 'OP_WHATSAPP', label: 'Op. Pago (WhatsApp)' },
]

// Sub-opciones simples (sin columna "Procesar en Neotel") de otros ítems
// del menú — mismo patrón que ITEMS_CASOS: aparecen solo si el ítem padre
// está tildado, y al tildar el padre parten todas seleccionadas.
const SUBITEMS: Record<string, { key: string; label: string; titulo: string }[]> = {
  'carga-mensual': [
    { key: 'carga_mensual_pl',   label: 'Pago Liviano',      titulo: 'Tipos en "Carga Mensual"' },
    { key: 'carga_mensual_refi', label: 'Refinanciamiento',  titulo: 'Tipos en "Carga Mensual"' },
  ],
  bases: [
    { key: 'bases_pl',   label: 'PL',   titulo: 'Tipos en "Bases"' },
    { key: 'bases_refi', label: 'REFI', titulo: 'Tipos en "Bases"' },
  ],
}

export function PermisosPanel() {
  const [abierto, setAbierto] = useState(false)
  const [usuarios, setUsuarios] = useState<string[]>([])
  const [usuario, setUsuario] = useState('')
  const [permisos, setPermisos] = useState<Record<string, boolean> | null>(null)
  const [cargando, setCargando] = useState(false)
  const [guardando, setGuardando] = useState(false)
  const [guardado, setGuardado] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!abierto) return
    fetch(`${API}/admin/permisos/usuarios`, { headers: authHeaders() })
      .then(r => r.json())
      .then(d => setUsuarios(d.usuarios ?? []))
      .catch(() => {})
  }, [abierto])

  const cargarUsuario = async (u: string) => {
    setUsuario(u)
    setPermisos(null)
    setError(null)
    setGuardado(false)
    if (!u.trim()) return
    setCargando(true)
    try {
      const res = await fetch(`${API}/admin/permisos/${encodeURIComponent(u.trim())}`, { headers: authHeaders() })
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || 'Error al cargar permisos')
      const data = await res.json()
      setPermisos(data.permisos)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setCargando(false)
    }
  }

  const toggle = (key: string) => {
    setPermisos(p => p ? { ...p, [key]: !p[key] } : p)
    setGuardado(false)
  }

  // Al activar un ítem con sub-opciones (Procesar → casos; Carga Mensual/
  // Bases → PL/REFI), esas sub-opciones aparecen por primera vez — parten
  // todas tildadas (visible todo), y desde ahí el admin destilda las que
  // quiera restringir. Al desactivar el ítem padre simplemente se ocultan
  // (sus valores quedan guardados tal cual para la próxima vez).
  const toggleConSubitems = (navKey: string, subitems: { key: string }[]) => {
    setPermisos(p => {
      if (!p) return p
      const activando = !p[navKey]
      const actualizado = { ...p, [navKey]: activando }
      if (activando) {
        for (const { key } of subitems) actualizado[key] = true
      }
      return actualizado
    })
    setGuardado(false)
  }

  const guardar = async () => {
    if (!permisos || !usuario.trim()) return
    setGuardando(true)
    setError(null)
    try {
      const res = await fetch(`${API}/admin/permisos/${encodeURIComponent(usuario.trim())}`, {
        method: 'PUT',
        headers: authHeaders(),
        body: JSON.stringify({ permisos }),
      })
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || 'Error al guardar')
      setGuardado(true)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setGuardando(false)
    }
  }

  const puedeProcesar = !!permisos?.['procesar']

  return (
    <div className="rounded-2xl border border-slate-200 bg-white shadow-sm">
      <button
        onClick={() => setAbierto(v => !v)}
        className="w-full flex items-center justify-between px-5 py-4"
      >
        <div className="flex items-center gap-2.5">
          <ShieldCheck size={18} className="text-indigo-500" />
          <p className="text-base font-semibold text-slate-800">Permisos por usuario</p>
        </div>
        <ChevronDown size={18} className={`text-slate-400 transition-transform ${abierto ? 'rotate-180' : ''}`} />
      </button>

      {abierto && (
        <div className="px-5 pb-5 space-y-4 border-t border-slate-100 pt-4 animate-fade-in">
          <p className="text-sm text-slate-400">
            Escribe el usuario (mismo que usa para iniciar sesión). Los administradores siempre ven todo — esto solo aplica a usuarios normales.
          </p>

          <div className="relative">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-300" />
            <input
              list="permisos-usuarios-conocidos"
              value={usuario}
              onChange={e => cargarUsuario(e.target.value)}
              placeholder="ej: nelson.paredes"
              className="w-full text-base pl-9 pr-3 py-2.5 rounded-lg border border-slate-200"
            />
            <datalist id="permisos-usuarios-conocidos">
              {usuarios.map(u => <option key={u} value={u} />)}
            </datalist>
          </div>

          {cargando && (
            <div className="flex justify-center py-6"><Loader2 size={20} className="animate-spin text-slate-300" /></div>
          )}

          {error && <p className="text-sm text-red-500">{error}</p>}

          {permisos && !cargando && (
            <div className="space-y-5 animate-fade-in">
              <div>
                <p className="text-sm font-semibold text-slate-500 mb-2">Secciones del menú</p>
                <div className="grid grid-cols-2 gap-2">
                  {ITEMS_NAV.map(({ key, label }) => (
                    <label key={key} className="flex items-center gap-2.5 text-sm text-slate-700 rounded-lg border border-slate-100 bg-slate-50 px-3 py-2.5 cursor-pointer">
                      <input
                        type="checkbox"
                        className="w-4 h-4 flex-shrink-0"
                        checked={!!permisos[key]}
                        onChange={() => key === 'procesar'
                          ? toggleConSubitems('procesar', ITEMS_CASOS)
                          : SUBITEMS[key]
                            ? toggleConSubitems(key, SUBITEMS[key])
                            : toggle(key)}
                      />
                      {label}
                    </label>
                  ))}
                </div>
              </div>

              {Object.entries(SUBITEMS).map(([navKey, items]) => permisos[navKey] && (
                <div key={navKey} className="animate-fade-in">
                  <p className="text-sm font-semibold text-slate-500 mb-2">{items[0].titulo}</p>
                  <div className="grid grid-cols-2 gap-2">
                    {items.map(({ key, label }) => (
                      <label key={key} className="flex items-center gap-2.5 text-sm text-slate-700 rounded-lg border border-slate-100 bg-slate-50 px-3 py-2.5 cursor-pointer">
                        <input
                          type="checkbox"
                          className="w-4 h-4 flex-shrink-0"
                          checked={!!permisos[key]}
                          onChange={() => toggle(key)}
                        />
                        {label}
                      </label>
                    ))}
                  </div>
                </div>
              ))}

              {puedeProcesar && (
                <div className="animate-fade-in">
                  <p className="text-sm font-semibold text-slate-500 mb-2">Casos en "Procesar"</p>
                  <div className="space-y-1.5">
                    {ITEMS_CASOS.map(({ key, label }) => (
                      <div key={key} className="flex items-center gap-3 rounded-lg border border-slate-100 bg-slate-50 px-3 py-2.5">
                        <label className="flex items-center gap-2.5 text-sm text-slate-700 cursor-pointer flex-1">
                          <input
                            type="checkbox"
                            className="w-4 h-4 flex-shrink-0"
                            checked={!!permisos[key]}
                            onChange={() => toggle(key)}
                          />
                          {label}
                        </label>
                        <label
                          className="flex items-center gap-2.5 text-sm cursor-pointer flex-shrink-0"
                          style={{ color: permisos[key] ? '#4f46e5' : '#cbd5e1' }}
                          title="Puede procesar en Neotel (si no, queda forzado a Solo proceso interno)"
                        >
                          <input
                            type="checkbox"
                            className="w-4 h-4 flex-shrink-0"
                            disabled={!permisos[key]}
                            checked={!!permisos[`${key}_neotel`]}
                            onChange={() => toggle(`${key}_neotel`)}
                          />
                          Procesar en Neotel
                        </label>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <div className="flex items-center gap-3 pt-1">
                <button
                  onClick={guardar}
                  disabled={guardando}
                  className="text-sm font-semibold rounded-lg px-4 py-2.5 text-white disabled:opacity-50"
                  style={{ backgroundColor: '#4f46e5' }}
                >
                  {guardando ? 'Guardando...' : 'Guardar permisos'}
                </button>
                {guardado && <CheckCircle2 size={18} className="text-emerald-500" />}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
