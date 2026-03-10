'use client'

import { useEffect, useState } from 'react'
import { RefreshCw, Search, Filter, Database, History } from 'lucide-react'
import { API } from '../lib/api'

interface Repetido {
  id: number
  rut: string
  tipo_caso: string
  fecha: string
}

const COLOR_CASO: Record<string, string> = {
  SAV:  'bg-blue-100 text-blue-700',
  AV:   'bg-purple-100 text-purple-700',
  REFI: 'bg-emerald-100 text-emerald-700',
  PL:   'bg-amber-100 text-amber-700',
}

const CASOS = ['SAV', 'AV', 'REFI', 'PL']

function getToken() {
  return typeof window !== 'undefined' ? sessionStorage.getItem('auth_token') : null
}

async function fetchRepetidosGuardados(tipoCaso: string, fechaDesde: string, fechaHasta: string): Promise<Repetido[]> {
  const params = new URLSearchParams({ limit: '500' })
  if (tipoCaso) params.set('tipo_caso', tipoCaso)
  const res = await fetch(`${API}/repetidos?${params}`, {
    headers: { Authorization: `Bearer ${getToken()}` },
  })
  if (!res.ok) return []
  const data: Repetido[] = await res.json()
  return data.filter(r => {
    const fecha = new Date(r.fecha)
    if (fechaDesde && fecha < new Date(fechaDesde)) return false
    if (fechaHasta && fecha > new Date(fechaHasta + 'T23:59:59')) return false
    return true
  })
}

async function fetchConsultaLive(caso: string): Promise<{ total: number; ruts: string[] }> {
  const res = await fetch(`${API}/consulta-repetidos?caso=${caso}`, {
    headers: { Authorization: `Bearer ${getToken()}` },
  })
  if (!res.ok) throw new Error('Error al consultar')
  return res.json()
}

type Tab = 'guardados' | 'consulta'

export function RepetidosPanel() {
  const [tab, setTab] = useState<Tab>('guardados')

  // Tab guardados
  const [guardados, setGuardados] = useState<Repetido[]>([])
  const [loadingGuardados, setLoadingGuardados] = useState(true)
  const [busqueda, setBusqueda] = useState('')
  const [tipoCaso, setTipoCaso] = useState('')
  const [fechaDesde, setFechaDesde] = useState('')
  const [fechaHasta, setFechaHasta] = useState('')

  // Tab consulta live
  const [casoLive, setCasoLive] = useState('SAV')
  const [resultadoLive, setResultadoLive] = useState<{ total: number; ruts: string[] } | null>(null)
  const [loadingLive, setLoadingLive] = useState(false)
  const [busquedaLive, setBusquedaLive] = useState('')
  const [errorLive, setErrorLive] = useState('')

  const cargarGuardados = async () => {
    setLoadingGuardados(true)
    const data = await fetchRepetidosGuardados(tipoCaso, fechaDesde, fechaHasta)
    setGuardados(data)
    setLoadingGuardados(false)
  }

  useEffect(() => { cargarGuardados() }, [tipoCaso, fechaDesde, fechaHasta])

  const consultarLive = async () => {
    setLoadingLive(true)
    setErrorLive('')
    setResultadoLive(null)
    try {
      const data = await fetchConsultaLive(casoLive)
      setResultadoLive(data)
    } catch (e: any) {
      setErrorLive(e.message)
    } finally {
      setLoadingLive(false)
    }
  }

  const filtradosGuardados = guardados.filter(r =>
    !busqueda || r.rut.includes(busqueda.trim())
  )

  const rutsFiltradosLive = resultadoLive?.ruts.filter(r =>
    !busquedaLive || r.includes(busquedaLive.trim())
  ) ?? []

  return (
    <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
      {/* Header con tabs */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-slate-100">
        <div className="flex items-center gap-1 rounded-lg border border-slate-200 p-0.5">
          <button
            onClick={() => setTab('guardados')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === 'guardados' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            <History size={12} /> Historial
          </button>
          <button
            onClick={() => setTab('consulta')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === 'consulta' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            <Database size={12} /> Consulta en vivo
          </button>
        </div>
      </div>

      {/* Tab Guardados */}
      {tab === 'guardados' && (
        <>
          <div className="px-5 py-3 border-b border-slate-100">
            <div className="flex flex-wrap gap-2 items-center">
              <div className="relative">
                <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400" />
                <input
                  type="text"
                  value={busqueda}
                  onChange={e => setBusqueda(e.target.value)}
                  placeholder="Buscar RUT..."
                  className="pl-8 pr-3 py-1.5 text-xs rounded-lg border border-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-200 w-36"
                />
              </div>
              <div className="relative">
                <Filter size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400" />
                <select
                  value={tipoCaso}
                  onChange={e => setTipoCaso(e.target.value)}
                  className="pl-8 pr-6 py-1.5 text-xs rounded-lg border border-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-200 appearance-none bg-white"
                >
                  <option value="">Todos los casos</option>
                  {CASOS.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
              <div className="flex items-center gap-1.5">
                <span className="text-xs text-slate-400">Desde</span>
                <input type="date" value={fechaDesde} onChange={e => setFechaDesde(e.target.value)}
                  className="px-2 py-1.5 text-xs rounded-lg border border-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-200" />
              </div>
              <div className="flex items-center gap-1.5">
                <span className="text-xs text-slate-400">Hasta</span>
                <input type="date" value={fechaHasta} onChange={e => setFechaHasta(e.target.value)}
                  className="px-2 py-1.5 text-xs rounded-lg border border-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-200" />
              </div>
              {(busqueda || tipoCaso || fechaDesde || fechaHasta) && (
                <button onClick={() => { setBusqueda(''); setTipoCaso(''); setFechaDesde(''); setFechaHasta('') }}
                  className="px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 text-slate-500 hover:bg-slate-50">
                  Limpiar
                </button>
              )}
              <span className="text-xs text-slate-400 ml-auto">{filtradosGuardados.length} registros</span>
              <button onClick={cargarGuardados} disabled={loadingGuardados}
                className="p-1.5 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100 disabled:opacity-40">
                <RefreshCw size={14} className={loadingGuardados ? 'animate-spin' : ''} />
              </button>
            </div>
          </div>
          <div className="overflow-x-auto">
            {loadingGuardados && guardados.length === 0 ? (
              <div className="flex justify-center py-10">
                <span className="spinner !border-slate-300 !border-t-slate-600" />
              </div>
            ) : filtradosGuardados.length === 0 ? (
              <p className="text-center text-slate-400 text-sm py-10">No hay registros</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-100 bg-slate-50">
                    {['RUT', 'Caso', 'Fecha'].map(h => (
                      <th key={h} className="px-4 py-2.5 text-left text-xs font-semibold text-slate-500 uppercase tracking-wide">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filtradosGuardados.map((r, i) => (
                    <tr key={r.id} className={`border-b border-slate-50 hover:bg-slate-50 transition-colors ${i % 2 === 0 ? '' : 'bg-slate-50/50'}`}>
                      <td className="px-4 py-2.5 font-mono text-xs text-slate-700">{r.rut}</td>
                      <td className="px-4 py-2.5">
                        <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${COLOR_CASO[r.tipo_caso] || 'bg-slate-100 text-slate-600'}`}>
                          {r.tipo_caso}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 text-slate-500 font-mono text-xs whitespace-nowrap">
                        {new Date(r.fecha).toLocaleString('es-CL')}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}

      {/* Tab Consulta Live */}
      {tab === 'consulta' && (
        <>
          <div className="px-5 py-3 border-b border-slate-100">
            <div className="flex flex-wrap gap-2 items-center">
              <div className="flex items-center gap-1 rounded-lg border border-slate-200 p-0.5">
                {CASOS.map(c => (
                  <button key={c} onClick={() => { setCasoLive(c); setResultadoLive(null) }}
                    className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                      casoLive === c ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-700'
                    }`}>
                    {c}
                  </button>
                ))}
              </div>
              <button
                onClick={consultarLive}
                disabled={loadingLive}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-blue-500 text-white hover:bg-blue-600 disabled:opacity-50 transition-colors"
              >
                {loadingLive ? <RefreshCw size={12} className="animate-spin" /> : <Database size={12} />}
                {loadingLive ? 'Consultando...' : 'Consultar'}
              </button>
              {resultadoLive && (
                <>
                  <span className="text-xs text-slate-400">{resultadoLive.total} repetidos en {casoLive}</span>
                  <div className="relative ml-auto">
                    <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400" />
                    <input type="text" value={busquedaLive} onChange={e => setBusquedaLive(e.target.value)}
                      placeholder="Buscar RUT..."
                      className="pl-8 pr-3 py-1.5 text-xs rounded-lg border border-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-200 w-36" />
                  </div>
                </>
              )}
            </div>
            {errorLive && <p className="text-xs text-red-500 mt-2">{errorLive}</p>}
          </div>

          <div className="overflow-x-auto">
            {!resultadoLive && !loadingLive ? (
              <p className="text-center text-slate-400 text-sm py-10">Selecciona un caso y presiona Consultar</p>
            ) : loadingLive ? (
              <div className="flex justify-center py-10">
                <span className="spinner !border-slate-300 !border-t-slate-600" />
              </div>
            ) : rutsFiltradosLive.length === 0 ? (
              <p className="text-center text-slate-400 text-sm py-10">No hay resultados</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-100 bg-slate-50">
                    <th className="px-4 py-2.5 text-left text-xs font-semibold text-slate-500 uppercase tracking-wide">RUT</th>
                  </tr>
                </thead>
                <tbody>
                  {rutsFiltradosLive.map((rut, i) => (
                    <tr key={rut} className={`border-b border-slate-50 hover:bg-slate-50 transition-colors ${i % 2 === 0 ? '' : 'bg-slate-50/50'}`}>
                      <td className="px-4 py-2.5 font-mono text-xs text-slate-700">{rut}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </div>
  )
}