import { getToken } from '../hooks/useAuth'

export const API = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'

// ── helper que inyecta el token automáticamente ──────────────
function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const token = getToken()
  return {
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...extra,
  }
}

function handleUnauthorized(res: Response) {
  if (res.status === 401) {
    sessionStorage.clear()
    window.location.href = '/login'
  }
  return res
}

// ── tus tipos sin cambios ────────────────────────────────────
export type CasoKey = 'SAV' | 'AV' | 'REFI' | 'PL' | 'PERDIDAS'

export interface ResultadoProceso {
  total_entrada: number
  total_repetidos: number
  total_bloqueados: number
  total_carga: number
  archivos?: { nombre: string; path: string }[]
  error?: string
}

export interface LogEntry {
  id: number
  tipo_caso: string
  fecha_proceso: string
  total_entrada: number
  total_repetidos: number
  total_bloqueados: number
  total_carga: number
  archivo_origen: string
}

export const CASOS: Record<CasoKey, {
  label: string
  color: string
  sftp: boolean
  descripcion: string
}> = {
  SAV:      { label: 'SAV',            color: '#0c90e6', sftp: false, descripcion: 'Leakage SAV — ECRM_0265' },
  AV:       { label: 'AV',             color: '#7c3aed', sftp: false, descripcion: 'Leakage AV — ECRM_0250' },
  REFI:     { label: 'REFI',           color: '#059669', sftp: true,  descripcion: 'Leakage REFI — ECRM_0289 (SFTP)' },
  PL:       { label: 'Pago Liviano',   color: '#d97706', sftp: true,  descripcion: 'Leakage PL — ECRM_0001 (SFTP)' },
  PERDIDAS: { label: 'Llamadas Perd.', color: '#dc2626', sftp: false, descripcion: 'Llamadas perdidas' },
}

// ── funciones con token ──────────────────────────────────────
export async function procesarCaso(caso: CasoKey, file?: File): Promise<ResultadoProceso> {
  if (file) {
    const form = new FormData()
    form.append('file', file)
    const res = await fetch(`${API}/procesar/${caso.toLowerCase()}`, {
      method: 'POST',
      headers: authHeaders(),
      body: form,
    })
    handleUnauthorized(res)
    if (!res.ok) throw new Error((await res.json()).detail || await res.text())
    return res.json()
  } else {
    const res = await fetch(`${API}/procesar/${caso.toLowerCase()}`, {
      method: 'POST',
      headers: authHeaders(),
    })
    handleUnauthorized(res)
    if (!res.ok) throw new Error((await res.json()).detail || await res.text())
    return res.json()
  }
}

export async function actualizarListaNegra(): Promise<any> {
  const res = await fetch(`${API}/lista-negra/actualizar`, {
    method: 'POST',
    headers: authHeaders(),
  })
  handleUnauthorized(res)
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function getLogs(limit = 50): Promise<LogEntry[]> {
  const res = await fetch(`${API}/logs?limit=${limit}`, {
    headers: authHeaders(),
  })
  handleUnauthorized(res)
  if (!res.ok) return []
  return res.json()
}

export async function getRutaBase(): Promise<string> {
  const res = await fetch(`${API}/config/ruta-base`, {
    headers: authHeaders(),
  })
  handleUnauthorized(res)
  if (!res.ok) return ''
  const data = await res.json()
  return data.ruta_base || ''
}

export async function setRutaBase(ruta: string): Promise<void> {
  await fetch(`${API}/config/ruta-base`, {
    method: 'PUT',
    headers: authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ ruta_base: ruta }),
  })
}