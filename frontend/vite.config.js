import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api/checkout": {
        target: "http://localhost:8088",
        changeOrigin: true,
      },
      "/api/orders": {
        target: "http://localhost:8088",
        changeOrigin: true,
      },
    },
  },
});
