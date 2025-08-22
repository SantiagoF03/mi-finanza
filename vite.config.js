import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwind from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwind()],
  // opcional para que el overlay no tape la pantalla:
  // server: { hmr: { overlay: false } },
})
