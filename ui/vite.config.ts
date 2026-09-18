import vue from '@vitejs/plugin-vue'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  envDir: '..',
  envPrefix: 'GOOGLE_MAPS_API_KEY',
  plugins: [vue()],
})
