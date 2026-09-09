// hooks/useAuth.ts
import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { API } from '../lib/api'

interface AuthUser { usuario: string; nombre: string; mail?: string; rol?: string; permisos?: Record<string, boolean> }

export const TOKEN_KEY = 'auth_token'
export const USER_KEY  = 'auth_user'

export function useAuth() {
  const router = useRouter()
  const [user, setUser] = useState<AuthUser | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const token   = sessionStorage.getItem(TOKEN_KEY)
    const userStr = sessionStorage.getItem(USER_KEY)
    if (token && userStr) {
      setUser(JSON.parse(userStr))
    } else {
      router.replace('/login')
    }
    setLoading(false)
  }, [])

  return { user, loading }
}

export async function login(usuario: string, password: string): Promise<AuthUser> {
  const res = await fetch(`${API}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ usuario, password }),
  })
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.detail ?? 'Error al iniciar sesion')
  }
  const data = await res.json()
  sessionStorage.setItem(TOKEN_KEY, data.access_token)
  const user: AuthUser = { usuario, nombre: data.nombre, rol: data.rol, permisos: data.permisos ?? {} }
  sessionStorage.setItem(USER_KEY, JSON.stringify(user))
  return user
}

export function logout() {
  sessionStorage.removeItem(TOKEN_KEY)
  sessionStorage.removeItem(USER_KEY)
  window.location.href = '/login'
}

export function getToken(): string | null {
  return sessionStorage.getItem(TOKEN_KEY)
}

// Admins ven todo (ya viene así en el mapa que manda el backend); si el
// item no está en el mapa (usuario viejo, item nuevo agregado después),
// se asume visible por defecto para no ocultar algo sin querer.
export function puedeVer(user: AuthUser | null, item: string): boolean {
  if (!user) return false
  return user.permisos?.[item] ?? true
}