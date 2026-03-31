import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/bq": {
        target: "http://localhost:9050",
        rewrite: (path) => path.replace(/^\/bq/, "/bigquery/v2"),
      },
    },
  },
});
