// Helper API centralizado — single-user app.
// VITE_API_KEY debe coincidir con API_KEY del backend.
// authHeaders() solo se usa en llamadas sensibles (approve/reject/settings/
// analysis/push-test); las lecturas públicas y push/subscribe van sin key.

// Con VITE_API_BASE vacío usa /api relativo: funciona en producción
// (Caddy reverse-proxy same-origin) y en dev (proxy de Vite → localhost:8000).
export const API = import.meta.env.VITE_API_BASE
  ? import.meta.env.VITE_API_BASE + '/api'
  : '/api'

const API_KEY = import.meta.env.VITE_API_KEY || ''

export function authHeaders(extra = {}) {
  return API_KEY ? { 'X-API-Key': API_KEY, ...extra } : extra
}
