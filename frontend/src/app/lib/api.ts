import { getToken } from '../hooks/useAuth'

export const API = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'

export type CasoKey = 'SAV' | 'AV' | 'REFI' | 'PL' | 'PERDIDAS'

export interface ResultadoProceso {
  total_entrada:    number
  total_repetidos:  number
  total_bloqueados: number
  total_carga:      number
  archivos?: { nombre: string; path: string }[]
  error?: string
}

export interface LogEntry {
  id:               number
  tipo_caso:        string
  fecha_proceso:    string
  total_entrada:    number
  total_repetidos:  number
  total_bloqueados: number
  total_carga:      number
  archivo_origen:   string
}

export const CASOS: Record<CasoKey, { label: string; color: string; sftp: boolean }> = {
  SAV:      { label: 'SAV',           color: '#0c90e6', sftp: true  },
  AV:       { label: 'AV',            color: '#7c3aed', sftp: true  },
  REFI:     { label: 'REFI',          color: '#059669', sftp: true  },
  PL:       { label: 'Pago Liviano',  color: '#d97706', sftp: true  },
  PERDIDAS: { label: 'Llamadas Perd.', color: '#dc2626', sftp: false },
}

// ── Helpers ──────────────────────────────────────────────────

function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const token = getToken()
  return { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...extra }
}

function handleUnauthorized(res: Response) {
  if (res.status === 401) {
    sessionStorage.clear()
    window.location.href = '/login'
  }
  return res
}

async function fetchJSON<T>(url: string, options?: RequestInit): Promise<T> {
  const res = handleUnauthorized(await fetch(url, options))
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || await res.text())
  return res.json()
}

// ── API calls ────────────────────────────────────────────────

export async function procesarCaso(caso: CasoKey, file?: File): Promise<ResultadoProceso> {
  const url = `${API}/procesar/${caso.toLowerCase()}`
  if (file) {
    const form = new FormData()
    form.append('file', file)
    return fetchJSON(url, { method: 'POST', headers: authHeaders(), body: form })
  }
  return fetchJSON(url, { method: 'POST', headers: authHeaders() })
}

export async function actualizarListaNegra(): Promise<any> {
  return fetchJSON(`${API}/lista-negra/actualizar`, { method: 'POST', headers: authHeaders() })
}

export async function getLogs(limit = 50): Promise<LogEntry[]> {
  const res = handleUnauthorized(await fetch(`${API}/logs?limit=${limit}`, { headers: authHeaders() }))
  if (!res.ok) return []
  return res.json()
}