// Helper API centralizado — single-user app.
// VITE_API_KEY debe coincidir con API_KEY del backend.
// authHeaders() solo se usa en llamadas sensibles (approve/reject/settings/
// analysis/push-test); las lecturas públicas y push/subscribe van sin key.

export const API = import.meta.env.VITE_API_BASE
  ? import.meta.env.VITE_API_BASE + '/api'
  : `${window.location.protocol}//${window.location.hostname}:8000/api`

const API_KEY = import.meta.env.VITE_API_KEY || ''

export function authHeaders(extra = {}) {
  return API_KEY ? { 'X-API-Key': API_KEY, ...extra } : extra
}
