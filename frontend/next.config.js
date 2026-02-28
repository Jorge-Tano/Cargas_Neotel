/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    return [
      {
        source: '/api/backend/:path*',
        destination: 'http://localhost:8000/:path*',
      },
    ]
  },
  experimental: {
    proxyTimeout: 120000, // 2 minutos
  },
}

module.exports = nextConfig