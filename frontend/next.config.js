/** @type {import('next').NextConfig} */
const isProd = process.env.NODE_ENV === 'production'

const nextConfig = {
  async rewrites() {
    return [
      {
        source: '/api/backend/:path*',
        destination: isProd
          ? 'http://172.31.7.234:8000/:path*'   // servidor produccion
          : 'http://localhost:8000/:path*',       // desarrollo local
      },
    ]
  },
  experimental: {
    proxyTimeout: 500000,
  },
}

module.exports = nextConfig