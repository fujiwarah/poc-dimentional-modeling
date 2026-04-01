import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import dbtLineagePlugin from "./vite-plugin-dbt-lineage";
import fs from "fs";

const dbtModelsDir = fs.existsSync("/dbt-models") ? "/dbt-models" : "../dbt/models";

export default defineConfig({
  plugins: [react(), dbtLineagePlugin(dbtModelsDir)],
  server: {
    proxy: {
      "/bq": {
        target: "http://localhost:9050",
        rewrite: (path) => path.replace(/^\/bq/, "/bigquery/v2"),
      },
    },
  },
});
