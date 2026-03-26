import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { TanStackRouterVite } from "@tanstack/router-plugin/vite";
import path from "path";

export default defineConfig({
  plugins: [
    TanStackRouterVite({
      routesDirectory: "./routes",
      generatedRouteTree: "./types/routeTree.gen.ts",
    }),
    react(),
    tailwindcss(),
  ],
  root: "./src/site_selection/ui",
  publicDir: path.resolve(__dirname, "./src/site_selection/ui/public"),
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src/site_selection/ui"),
    },
  },
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8811",
        changeOrigin: true,
      },
    },
  },
});
