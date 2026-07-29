import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  root: "web",
  plugins: [react()],
  build: {
    outDir: "../dist/web",
    emptyOutDir: true,
    target: "es2022",
    cssCodeSplit: true,
    sourcemap: false
  },
  server: {
    port: 4173,
    proxy: {
      "/api": "http://localhost:8080"
    }
  }
});
