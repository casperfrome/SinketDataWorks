import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
export default defineConfig(({ mode }) => {
  const target = process.env.VITE_API_TARGET || loadEnv(mode, process.cwd(), "VITE_").VITE_API_TARGET || "http://127.0.0.1:8080";
  const proxy = { "/api": target, "/actuator": target };
  return { plugins: [react()], server: { host: "127.0.0.1", port: 5173, strictPort: true, proxy }, preview: { host: "127.0.0.1", proxy }, build: { chunkSizeWarningLimit: 2000 } };
});
