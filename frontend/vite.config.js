import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Vite outputs to ../build, which the FastAPI backend serves as static files.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../build',
    emptyOutDir: true,
    sourcemap: false,
  },
  server: {
    port: 3000,
    proxy: {
      '/api': { target: 'http://localhost:8000', changeOrigin: true },
    },
  },
});
