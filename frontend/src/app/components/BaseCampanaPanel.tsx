'use client'

import { useState, useEffect, useRef } from 'react'
import { Plus, Loader2, X, Database, ChevronRight } from 'lucide-react'
import { API } from '../lib/api'
import { getToken } from '../hooks/useAuth'

function authHeaders(): Record<string, string> {
  const token = getToken()
  return { ...(token ? { Authorization: `Bearer ${token}` } : {}), 'Content-Type': 'application/json' }
}

interface BaseResumen {
  iddatabase: number
  descripcion: string
  estado: string
  fhalta: string | null
  contactos: number
}

interface BaseValores {
  estado: string
  diferencia_horaria: number
  permite_agregar: boolean
  intentos_discador: number
  autocerrar_contactos: boolean
  intentos_totales: number
  autocerrar_contactos_totales: boolean
  intentos_dia: number
  intentos_mes: number
  orden_discado: string
  telefonos_a_discar: string
}

const VALORES_VACIOS: BaseValores = {
  estado: '1',
  diferencia_horaria: 0,
  permite_agregar: false,
  intentos_discador: 100,
  autocerrar_contactos: false,
  intentos_totales: 120,
  autocerrar_contactos_totales: false,
  intentos_dia: 9999,
  intentos_mes: 9999,
  orden_discado: 'INTOBM011 DESC, INTOBM011 DESC, INTOBM011 DESC',
  telefonos_a_discar: '1,2,3',
}

// Campos disponibles para "Orden discado" — columnas reales de CONTACTOS +
// DB_CONTACTOS (sys.columns), igual catálogo que usa el selector de Neotel.
const CAMPOS_ORDEN_DISCADO = [
  'ACD_CONTADOR', 'ACD_ESTADO', 'ACD_FECHA', 'ACD_SUB_ESTADO', 'ACD_TEL',
  'CONTADOR_DIA', 'CONTADOR_LLAMADOS_TEL', 'CONTADOR_MES', 'CRM_CONTADOR',
  'DECDEUDA40', 'FECFECHAMONITOR', 'FECFECHASUPER', 'FH_CIERRE', 'GESTIONADO',
  'IDCUOTA', 'IDDATABASE', 'IDDATABASE_ORI', 'IDINTERNO', 'IDLOTE', 'IDVENTA_AGENDA',
  'INTCTAOUP', 'INTCTASER', 'INTDEUDA40', 'INTFECHAVCTO', 'INTOBM011', 'INTORDENDISCADO',
  'INTPAG009', 'INTPORCENTAJEDEUDA',
  'LAST_ACD_TEL', 'LAST_CRM_TEL', 'LSTLISTASERIE', 'LSTMOTIVOLLAMADA',
  'MOTIVO_TRAIDA', 'OBSERVACIONES', 'ORDEN', 'REFERENTE', 'SUBCATEGORIA', 'SUBCATEGORIA_1',
  'TELCONTACTOEFECTIVO', 'TELTELEFONO1', 'TELTELEFONO2', 'TELTELEFONO3', 'TELTELEFONO4',
  'TELTELEFONO5', 'TELTELEFONO6', 'TELTELEFONO8', 'TELTELEFONO9', 'TELTELEFONO10', 'TELTELEFONO11',
  'TELTELEFONOADICIONAL1', 'TELTELEFONOADICIONAL2', 'TELTELEFONOADICIONAL3',
  'TELTELEFONOLIDERBCI', 'TELTELEFONOPARTICULAR',
  'TS', 'TS_CONTADOR', 'USUARIO', 'USUARIO_PREASIGNADO',
  'TXTABONO', 'TXTAPARTADOAEREOPARTICULAR', 'TXTAPARTADOCOMERCIAL', 'TXTAPELLIDOMATERNO',
  'TXTAPELLIDOPATERNO', 'TXTAT001', 'TXTAT002', 'TXTAT003', 'TXTAT004', 'TXTAT005', 'TXTAT006', 'TXTAT007',
  'TXTAV', 'TXTBDD', 'TXTCAPITALADEUDADO', 'TXTCARGODEUDOR', 'TXTCIUDADCOMERCIAL', 'TXTCIUDADPARTICULAR',
  'TXTCLASIFICACION', 'TXTCODAREA', 'TXTCODIGOAREATELEFONO', 'TXTCODIGOBANCO', 'TXTCODIGOCOMUNA',
  'TXTCODIGOCOMUNAPARTICULAR', 'TXTCODIGOSEXO', 'TXTCOMUNA', 'TXTCOMUNAA', 'TXTCOMUNACOMERCIAL',
  'TXTCOMUNAPARTICULAR', 'TXTCUOTASUGERIDA', 'TXTDESCUENTOTASA', 'TXTDIGITO', 'TXTDIRECCIONCOMERCIAL',
  'TXTDIRECCIONPARTICULAR', 'TXTEMPRESADEUDOR', 'TXTESTADO', 'TXTESTADOCALIDAD', 'TXTESTADOCIVIL',
  'TXTEXTENSIÓNANEXO', 'TXTEXTENSIONANEXOPARTICULAR', 'TXTFECHACARGA', 'TXTFECHAEXCLUSION',
  'TXTFECHAINICIO', 'TXTFECHANACIMIENTO', 'TXTFECHATERMINO', 'TXTFINLLAMADAC', 'TXTIDEFMANDANTE',
  'TXTIDENTIFDEUDOR', 'TXTINCREMENTAL', 'TXTINDICADORLEALTAD', 'TXTINDICATIVOCIUDAD',
  'TXTINDICATIVOCOMERCIAL', 'TXTINICIOLLAMADAC', 'TXTMARCAESTRATEGIA', 'TXTMARCAPROPENSION',
  'TXTMARCAREGION', 'TXTMARCAREGIONES', 'TXTMONITOR', 'TXTMONTOFINAL', 'TXTMORA', 'TXTNOMBRE',
  'TXTNOVEDAD', 'TXTOBM004', 'TXTOBM012', 'TXTOBM013', 'TXTOBM015', 'TXTOBM019', 'TXTOBM020',
  'TXTPAG007', 'TXTPAG014', 'TXTPAG017', 'TXTPAGO', 'TXTPAGOMINIMO', 'TXTPAISCOMERCIAL',
  'TXTPAISPARTICULAR', 'TXTPIE', 'TXTPM0', 'TXTPRODUCTO', 'TXTPROFESIONDEUDOR', 'TXTPROPENSION',
  'TXTPROPENSIONMORA', 'TXTRANGO', 'TXTREGIONCOMERCIAL', 'TXTREGIONPARTICULAR', 'TXTRUT',
  'TXTRUTBUENOMALOC', 'TXTRUTENTRANTE', 'TXTSAV', 'TXTSECUENCIA', 'TXTSUCURSALDEUDOR', 'TXTTASA',
  'TXTTASA2448', 'TXTTASA623', 'TXTTELEFONOCOMERCIAL', 'TXTTELEFONOCOMPRA', 'TXTTIPOBASE',
  'TXTTIPOCOMERCIAL', 'TXTTIPODOCUMENTO', 'TXTTIPOPARTICULAR', 'TXTTIPOPROPENSION', 'TXTTM1',
  'TXTVENCIMENTOTARJETA',
].sort()

function _etiquetaCampo(nombre: string): string {
  const m = nombre.match(/^(TXT|TEL|INT|DEC|FEC|LST)(.+)$/)
  return m ? m[2] : nombre
}

function CampoOrdenInput({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const [abierto, setAbierto] = useState(false)
  const filtro = value.trim().toLowerCase()
  const sugerencias = (filtro
    ? CAMPOS_ORDEN_DISCADO.filter(c => c.toLowerCase().includes(filtro) || _etiquetaCampo(c).toLowerCase().includes(filtro))
    : CAMPOS_ORDEN_DISCADO
  ).slice(0, 30)

  return (
    <div className="relative flex-1 min-w-0">
      <input
        value={value}
        onChange={e => { onChange(e.target.value); setAbierto(true) }}
        onFocus={() => setAbierto(true)}
        onBlur={() => setTimeout(() => setAbierto(false), 150)}
        placeholder="Escriba aquí..."
        className="w-full min-w-0 text-sm px-3 py-2 rounded-lg border border-slate-200 font-mono"
      />
      {abierto && sugerencias.length > 0 && (
        <div className="absolute z-20 mt-1 w-full max-h-48 overflow-y-auto rounded-lg border border-slate-200 bg-white shadow-lg">
          {sugerencias.map(c => (
            <button
              key={c}
              type="button"
              onMouseDown={() => { onChange(c); setAbierto(false) }}
              className="w-full text-left px-3 py-1.5 text-xs hover:bg-slate-50 text-slate-600"
            >
              {_etiquetaCampo(c)}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

interface NivelOrden { campo: string; direccion: 'ASC' | 'DESC' }

function parseOrdenDiscado(raw: string): NivelOrden[] {
  const partes = raw.split(',').map(p => p.trim()).filter(Boolean)
  if (partes.length === 0) return [{ campo: '', direccion: 'DESC' }]
  return partes.map(p => {
    const m = p.match(/^(.*?)\s+(ASC|DESC)$/i)
    return m ? { campo: m[1].trim(), direccion: m[2].toUpperCase() as 'ASC' | 'DESC' } : { campo: p, direccion: 'DESC' as const }
  })
}

function serializarOrdenDiscado(niveles: NivelOrden[]): string {
  return niveles.filter(n => n.campo.trim()).map(n => `${n.campo.trim()} ${n.direccion}`).join(', ')
}

function OrdenDiscadoBuilder({ value, onChange, color }: { value: string; onChange: (v: string) => void; color: string }) {
  const [niveles, setNiveles] = useState<NivelOrden[]>(() => parseOrdenDiscado(value))

  useEffect(() => { onChange(serializarOrdenDiscado(niveles)) }, [niveles]) // eslint-disable-line react-hooks/exhaustive-deps

  const actualizar = (i: number, cambio: Partial<NivelOrden>) =>
    setNiveles(prev => prev.map((n, idx) => idx === i ? { ...n, ...cambio } : n))
  const agregar = () => setNiveles(prev => [...prev, { campo: '', direccion: 'DESC' }])
  const quitar = (i: number) => setNiveles(prev => prev.filter((_, idx) => idx !== i))

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2">
        {niveles.map((n, i) => (
          <div key={i} className="flex items-center gap-1 flex-1 min-w-[160px]">
            <CampoOrdenInput value={n.campo} onChange={v => actualizar(i, { campo: v })} />
            <select value={n.direccion} onChange={e => actualizar(i, { direccion: e.target.value as 'ASC' | 'DESC' })}
              className="text-sm px-2 py-2 rounded-lg border border-slate-200 flex-shrink-0">
              <option value="ASC">ASC</option>
              <option value="DESC">DESC</option>
            </select>
            {niveles.length > 1 && (
              <button type="button" onClick={() => quitar(i)} className="text-slate-300 hover:text-red-500 flex-shrink-0">
                <X size={14} />
              </button>
            )}
          </div>
        ))}
      </div>
      <button type="button" onClick={agregar} className="text-xs font-medium" style={{ color }}>
        + Agregar nivel
      </button>
    </div>
  )
}

const OPCIONES_ESTADO = [
  { value: '0', label: '0 - no procesados + reprogramados prioritarios + no prioritarios' },
  { value: '1', label: '1 - no procesados + reprogramados prioritarios' },
  { value: '2', label: '2 - no procesados' },
  { value: '3', label: '3 - reprogramados prioritarios + no prioritarios' },
  { value: '4', label: '4 - reprogramados prioritarios' },
  { value: '5', label: '5 - ninguno' },
]

interface ProgressStep { step: string; elapsed: number }

function Campo({ label, span2, children }: { label: string; span2?: boolean; children: React.ReactNode }) {
  return (
    <div className={span2 ? 'col-span-2' : ''}>
      <label className="text-xs font-semibold text-slate-500">{label}</label>
      <div className="mt-1">{children}</div>
    </div>
  )
}

function BaseForm({
  tipo, color, modo, descripcionInicial, valoresIniciales, onCancelar, onListo,
}: {
  tipo: 'pl' | 'refi'
  color: string
  modo: 'crear' | 'editar'
  descripcionInicial?: string
  valoresIniciales: BaseValores
  onCancelar: () => void
  onListo: () => void
}) {
  const [descripcion, setDescripcion] = useState(descripcionInicial ?? '')
  const [valores, setValores] = useState<BaseValores>(valoresIniciales)
  const [fase, setFase] = useState<'idle' | 'enviando' | 'done'>('idle')
  const [steps, setSteps] = useState<ProgressStep[]>([])
  const [error, setError] = useState<string | null>(null)
  const esRef = useRef<EventSource | null>(null)

  const set = <K extends keyof BaseValores>(campo: K, valor: BaseValores[K]) =>
    setValores(v => ({ ...v, [campo]: valor }))

  const enviar = async () => {
    setFase('enviando')
    setError(null)
    setSteps([{ step: 'Iniciando...', elapsed: 0 }])
    try {
      const url = modo === 'crear'
        ? `${API}/carga-mensual/${tipo}/bases`
        : `${API}/carga-mensual/${tipo}/bases/${(valoresIniciales as any).iddatabase}`
      const res = await fetch(url, {
        method: modo === 'crear' ? 'POST' : 'PUT',
        headers: authHeaders(),
        body: JSON.stringify(modo === 'crear' ? { ...valores, descripcion } : valores),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'Error al enviar')
      }
      const { job_id } = await res.json()
      const es = new EventSource(`${API}/jobs/${job_id}/stream`)
      esRef.current = es
      es.onmessage = (e) => {
        if (!e.data || e.data.startsWith(':')) return
        try {
          const msg: any = JSON.parse(e.data)
          setSteps(prev => {
            const base = prev.length === 1 && prev[0].step === 'Iniciando...' ? [] : prev
            return [...base, { step: msg.step, elapsed: msg.elapsed }]
          })
          if (msg.done) {
            es.close()
            esRef.current = null
            setFase('done')
            if (msg.error) setError(msg.error)
            else onListo()
          }
        } catch { }
      }
      es.onerror = () => {
        es.close()
        esRef.current = null
        setFase('done')
        setError('Error de conexión')
      }
    } catch (e: any) {
      setError(e.message)
      setFase('done')
    }
  }

  const faltantes = [
    ...(modo === 'crear' && !descripcion.trim() ? ['Descripción'] : []),
    ...(!valores.orden_discado.trim() ? ['Orden discado'] : []),
    ...(!valores.telefonos_a_discar.trim() ? ['Teléf. a discar'] : []),
  ]
  const listoParaEnviar = faltantes.length === 0

  return (
    <div className="rounded-2xl border p-5 space-y-4" style={{ borderColor: `${color}30`, backgroundColor: `${color}05` }}>
      <div className="flex items-center justify-between">
        <p className="text-sm font-bold" style={{ color }}>
          {modo === 'crear' ? 'Crear base nueva' : `Editar base Nº ${(valoresIniciales as any).iddatabase}`}
        </p>
        <button onClick={onCancelar} className="text-slate-400 hover:text-slate-600"><X size={16} /></button>
      </div>

      {fase === 'idle' && (
        <div className="animate-fade-in">
          {/* Misma disposición 2 columnas que "Datos de la base de datos" en Neotel */}
          <div className="grid grid-cols-2 gap-x-6 gap-y-4">
            {modo === 'editar' ? (
              <Campo label="Nº">
                <input value={(valoresIniciales as any).iddatabase} disabled
                  className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200 bg-slate-50 text-slate-400" />
              </Campo>
            ) : <div />}
            <Campo label="Descripción">
              <input value={descripcion} onChange={e => setDescripcion(e.target.value)}
                disabled={modo === 'editar'}
                placeholder={`Ej: BASE ${tipo.toUpperCase()} LEAKAGE SEPTIEMBRE 2026`}
                className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200 disabled:bg-slate-50 disabled:text-slate-400" />
            </Campo>

            <Campo label="Dif. horaria">
              <input type="number" value={valores.diferencia_horaria}
                onChange={e => set('diferencia_horaria', Number(e.target.value))}
                className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200" />
            </Campo>
            <Campo label="Características">
              <input value="" disabled placeholder="(sin uso)"
                className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200 bg-slate-50 text-slate-300" />
            </Campo>

            <Campo label="Permite altas">
              <select value={valores.permite_agregar ? 'si' : 'no'} onChange={e => set('permite_agregar', e.target.value === 'si')}
                className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200">
                <option value="no">No</option>
                <option value="si">Sí</option>
              </select>
            </Campo>
            <Campo label="Intentos disc.">
              <div className="flex items-center gap-2">
                <input type="number" value={valores.intentos_discador}
                  onChange={e => set('intentos_discador', Number(e.target.value))}
                  className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200" />
                <label className="flex items-center gap-1.5 text-xs text-slate-500 whitespace-nowrap">
                  <input type="checkbox" checked={valores.autocerrar_contactos} onChange={e => set('autocerrar_contactos', e.target.checked)} />
                  Cerrar auto.
                </label>
              </div>
            </Campo>

            <Campo label="Intentos totales">
              <div className="flex items-center gap-2">
                <input type="number" value={valores.intentos_totales}
                  onChange={e => set('intentos_totales', Number(e.target.value))}
                  className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200" />
                <label className="flex items-center gap-1.5 text-xs text-slate-500 whitespace-nowrap">
                  <input type="checkbox" checked={valores.autocerrar_contactos_totales} onChange={e => set('autocerrar_contactos_totales', e.target.checked)} />
                  Cerrar auto.
                </label>
              </div>
            </Campo>
            <Campo label="Contactos a procesar">
              <select value={valores.estado} onChange={e => set('estado', e.target.value)}
                className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200">
                {OPCIONES_ESTADO.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </Campo>

            <Campo label="Intentos máximos por día">
              <input type="number" value={valores.intentos_dia}
                onChange={e => set('intentos_dia', Number(e.target.value))}
                className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200" />
            </Campo>
            <Campo label="Intentos máximos por mes">
              <input type="number" value={valores.intentos_mes}
                onChange={e => set('intentos_mes', Number(e.target.value))}
                className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200" />
            </Campo>

            <Campo label="Orden discado" span2>
              <OrdenDiscadoBuilder value={valores.orden_discado} onChange={v => set('orden_discado', v)} color={color} />
            </Campo>

            <Campo label="Teléf. a discar (predict.)" span2>
              <input value={valores.telefonos_a_discar} onChange={e => set('telefonos_a_discar', e.target.value)}
                placeholder="Ej: 1,2,3"
                className="w-full text-sm px-3 py-2 rounded-lg border border-slate-200 font-mono" />
            </Campo>
          </div>

          <button
            onClick={enviar}
            disabled={!listoParaEnviar}
            className="w-full mt-5 py-2.5 rounded-xl text-white text-sm font-semibold disabled:opacity-40 transition-all flex items-center justify-center gap-2"
            style={{
              background: listoParaEnviar ? `linear-gradient(135deg, ${color}, ${color}cc)` : '#e2e8f0',
              color: listoParaEnviar ? 'white' : '#94a3b8',
            }}
          >
            <ChevronRight size={15} /> {modo === 'crear' ? 'Crear base' : 'Guardar cambios'}
          </button>
          {faltantes.length > 0 && (
            <p className="text-xs text-amber-600 mt-1.5">Falta completar: {faltantes.join(', ')}</p>
          )}
        </div>
      )}

      {fase === 'enviando' && (
        <div className="space-y-1.5 animate-fade-in">
          {steps.map((s, i) => (
            <div key={i} className="flex items-center gap-2">
              {i === steps.length - 1
                ? <Loader2 size={11} className="animate-spin flex-shrink-0" style={{ color }} />
                : <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: `${color}30`, border: `1.5px solid ${color}` }} />}
              <span className="text-xs flex-1 break-all text-slate-500">{s.step}</span>
            </div>
          ))}
        </div>
      )}

      {fase === 'done' && error && (
        <div className="space-y-3 animate-fade-in">
          <div className="rounded-xl px-3 py-2.5 text-xs" style={{ backgroundColor: '#fef2f2', border: '1px solid #fecaca', color: '#dc2626' }}>
            <span className="font-semibold">Error: </span>{error}
          </div>
          <button onClick={() => setFase('idle')} className="w-full text-xs py-2 rounded-xl font-medium"
            style={{ color, backgroundColor: `${color}08`, border: `1px solid ${color}20` }}>
            Reintentar
          </button>
        </div>
      )}
    </div>
  )
}

export function BaseCampanaPanel({ tipo, label, color }: { tipo: 'pl' | 'refi'; label: string; color: string }) {
  const [bases, setBases] = useState<BaseResumen[]>([])
  const [cargando, setCargando] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [formulario, setFormulario] = useState<{ modo: 'crear' | 'editar'; valores: BaseValores; descripcion?: string } | null>(null)
  const [cargandoDetalle, setCargandoDetalle] = useState<number | null>(null)

  const cargarBases = async () => {
    setCargando(true)
    setError(null)
    try {
      const res = await fetch(`${API}/carga-mensual/${tipo}/bases`, { headers: authHeaders() })
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || 'Error al listar bases')
      const data = await res.json()
      setBases(data.bases ?? [])
    } catch (e: any) {
      setError(e.message)
    } finally {
      setCargando(false)
    }
  }

  useEffect(() => { cargarBases() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const abrirEditar = async (iddatabase: number) => {
    setCargandoDetalle(iddatabase)
    try {
      const res = await fetch(`${API}/carga-mensual/${tipo}/bases/${iddatabase}`, { headers: authHeaders() })
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || 'Error al obtener la base')
      const base = await res.json()
      setFormulario({ modo: 'editar', valores: base })
    } catch (e: any) {
      setError(e.message)
    } finally {
      setCargandoDetalle(null)
    }
  }

  if (cargando) {
    return (
      <div className="rounded-2xl border border-slate-200 bg-white p-5 flex items-center justify-center py-16">
        <Loader2 size={20} className="animate-spin text-slate-300" />
      </div>
    )
  }

  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-5 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Database size={15} style={{ color }} />
          <p className="text-sm font-bold text-slate-700">{label}</p>
        </div>
        {!formulario && (
          <button
            onClick={() => setFormulario({ modo: 'crear', valores: VALORES_VACIOS })}
            className="inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-semibold transition-all"
            style={{ backgroundColor: `${color}10`, border: `1px solid ${color}30`, color }}
          >
            <Plus size={13} /> Crear base nueva
          </button>
        )}
      </div>

      {formulario && (
        <BaseForm
          tipo={tipo}
          color={color}
          modo={formulario.modo}
          descripcionInicial={formulario.descripcion}
          valoresIniciales={formulario.valores}
          onCancelar={() => setFormulario(null)}
          onListo={() => { setFormulario(null); cargarBases() }}
        />
      )}

      {!formulario && (
        <>
          {error && <p className="text-xs text-red-500">{error}</p>}
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-400 text-left border-b border-slate-100">
                  <th className="py-2 font-semibold">Nº</th>
                  <th className="py-2 font-semibold">Descripción</th>
                  <th className="py-2 font-semibold">Estado</th>
                  <th className="py-2 font-semibold">Fecha alta</th>
                  <th className="py-2 font-semibold">Contactos</th>
                  <th className="py-2"></th>
                </tr>
              </thead>
              <tbody>
                {bases.map(b => (
                  <tr key={b.iddatabase} className="border-b border-slate-50 hover:bg-slate-50">
                    <td className="py-2 tabular-nums">{b.iddatabase}</td>
                    <td className="py-2">{b.descripcion}</td>
                    <td className="py-2 tabular-nums">{b.estado}</td>
                    <td className="py-2 text-slate-400">{b.fhalta ? new Date(b.fhalta).toLocaleDateString('es-CL') : '—'}</td>
                    <td className="py-2 tabular-nums">{b.contactos}</td>
                    <td className="py-2 text-right">
                      <button
                        onClick={() => abrirEditar(b.iddatabase)}
                        disabled={cargandoDetalle === b.iddatabase}
                        className="text-xs font-medium disabled:opacity-40"
                        style={{ color }}
                      >
                        {cargandoDetalle === b.iddatabase ? <Loader2 size={12} className="animate-spin" /> : 'Editar'}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
