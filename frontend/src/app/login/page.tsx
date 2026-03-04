'use client'

import { useState, FormEvent } from 'react'
import { useRouter } from 'next/navigation'
import { login } from '../hooks/useAuth'
import { Lock, User, LogIn, Eye, EyeOff } from 'lucide-react'

export default function LoginPage() {
  const router = useRouter()
  const [usuario, setUsuario] = useState('')
  const [password, setPassword] = useState('')
  const [mostrarPw, setMostrarPw] = useState(false)
  const [cargando, setCargando] = useState(false)
  const [error, setError] = useState('')

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault()
    setError('')
    setCargando(true)
    console.log('Intentando login con:', usuario)  // <- agrega esto
    try {
      await login(usuario.trim(), password)
      router.replace('/')
    } catch (err: any) {
      setError(err.message ?? 'No se pudo iniciar sesion')
    } finally {
      setCargando(false)
    }
  }

  return (
    <div className='min-h-screen bg-slate-50 flex items-center justify-center p-4'>
      <div className='w-full max-w-sm'>
        <div className='text-center mb-8'>
          <div className='w-12 h-12 rounded-2xl bg-blue-500 flex items-center justify-center mx-auto mb-4 shadow-lg shadow-blue-200'>
            <Lock size={22} className='text-white' />
          </div>
          <h1 className='text-xl font-bold text-slate-800'>Acceso restringido</h1>
          <p className='text-sm text-slate-400 mt-1'>Usa tus credenciales de red</p>
        </div>
        <div className='bg-white rounded-2xl border border-slate-200 shadow-sm p-6 space-y-4'>
          {error && <div className='rounded-xl bg-red-50 border border-red-100 px-4 py-3 text-sm text-red-600'>{error}</div>}
          <form onSubmit={handleSubmit} className='space-y-4'>
            <div>
              <label className='block text-xs font-semibold text-slate-600 mb-1.5'>Usuario de red</label>
              <div className='relative'>
                <User size={14} className='absolute left-3 top-1/2 -translate-y-1/2 text-slate-400' />
                <input type='text' value={usuario} onChange={e => setUsuario(e.target.value)} className='w-full rounded-xl border border-slate-200 pl-9 pr-3 py-2.5 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-200' placeholder='tu.usuario' required />
              </div>
            </div>
            <div>
              <label className='block text-xs font-semibold text-slate-600 mb-1.5'>Contrasena</label>
              <div className='relative'>
                <Lock size={14} className='absolute left-3 top-1/2 -translate-y-1/2 text-slate-400' />
                <input type={mostrarPw ? 'text' : 'password'} value={password} onChange={e => setPassword(e.target.value)} className='w-full rounded-xl border border-slate-200 pl-9 pr-9 py-2.5 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-200' placeholder='••••••••' required />
                <button type='button' onClick={() => setMostrarPw(!mostrarPw)} className='absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600'>
                  {mostrarPw ? <EyeOff size={14} /> : <Eye size={14} />}
                </button>
              </div>
            </div>
            <button type='submit' disabled={cargando || !usuario || !password} className='w-full flex items-center justify-center gap-2 rounded-xl py-2.5 text-sm font-semibold text-white bg-blue-500 hover:bg-blue-600 disabled:opacity-50 transition-all mt-2'>
              {cargando ? <span className='spinner' /> : <LogIn size={15} />}
              {cargando ? 'Verificando...' : 'Ingresar'}
            </button>
          </form>
        </div>
        <p className='text-center text-xs text-slate-400 mt-6'>Acceso solo para personal autorizado</p>
      </div>
    </div>
  )
}