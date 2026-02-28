import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Neotel — Gestión de Cargas',
  description: 'Sistema de automatización de cargas Neotel',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="es">
      <body>{children}</body>
    </html>
  )
}