import fs from "fs";
import path from "path";
import type { Plugin } from "vite";

const VIRTUAL_MODULE_ID = "virtual:dbt-lineage";
const RESOLVED_ID = "\0" + VIRTUAL_MODULE_ID;

interface ModelDeps {
  refs: string[];
  sources: string[];
}

function parseDbtModels(modelsDir: string): Record<string, ModelDeps> {
  const graph: Record<string, ModelDeps> = {};

  function walk(dir: string) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.name.endsWith(".sql")) {
        const modelName = entry.name.replace(/\.sql$/, "");
        const content = fs.readFileSync(full, "utf-8");

        const refs = [
          ...content.matchAll(/\{\{\s*ref\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}/g),
        ].map((m) => m[1]);
        const sources = [
          ...content.matchAll(
            /\{\{\s*source\(\s*['"][^'"]+['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}/g,
          ),
        ].map((m) => m[1]);

        graph[modelName] = {
          refs: [...new Set(refs)],
          sources: [...new Set(sources)],
        };
      }
    }
  }

  walk(modelsDir);
  return graph;
}

export default function dbtLineagePlugin(modelsDir: string): Plugin {
  const resolved = path.resolve(modelsDir);

  return {
    name: "dbt-lineage",
    resolveId(id) {
      if (id === VIRTUAL_MODULE_ID) return RESOLVED_ID;
    },
    load(id) {
      if (id === RESOLVED_ID) {
        const graph = parseDbtModels(resolved);
        return `export default ${JSON.stringify(graph)};`;
      }
    },
  };
}
