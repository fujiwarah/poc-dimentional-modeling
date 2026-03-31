import { useState, useCallback } from "react";
import { runQuery, type QueryResult } from "../lib/bq.ts";
import DataTable from "./DataTable.tsx";

const SAMPLE_QUERY = `SELECT
  d.year,
  d.month,
  d.month_name,
  COUNT(DISTINCT f.order_id) AS order_count,
  SUM(f.net_amount) AS total_sales
FROM dwh.fact_sales f
INNER JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY 1, 2, 3
ORDER BY 1, 2`;

export default function QueryEditor() {
  const [sql, setSql] = useState(SAMPLE_QUERY);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const execute = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await runQuery(sql);
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
      setResult(null);
    } finally {
      setLoading(false);
    }
  }, [sql]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
        e.preventDefault();
        execute();
      }
    },
    [execute],
  );

  return (
    <div className="flex flex-col gap-3 h-full">
      <div className="relative">
        <textarea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          spellCheck={false}
          className="w-full h-48 bg-white dark:bg-zinc-900 border rounded-lg p-3 font-mono text-sm text-zinc-800 dark:text-zinc-200 resize-y focus:outline-none focus:ring-1 focus:ring-blue-500/50 focus:border-blue-500/50"
          placeholder="SELECT ..."
        />
        <div className="absolute bottom-3 right-3 text-xs text-zinc-400 dark:text-zinc-600">
          ⌘ + Enter
        </div>
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={execute}
          disabled={loading || !sql.trim()}
          className="px-4 py-1.5 bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:hover:bg-blue-600 rounded-md text-sm font-medium text-white transition-colors"
        >
          {loading ? "Running..." : "Run"}
        </button>
        {result && (
          <span className="text-xs text-zinc-500 dark:text-zinc-400">
            {result.totalRows} rows
          </span>
        )}
      </div>

      {error && (
        <div className="bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-900/50 rounded-lg p-3 text-sm text-red-600 dark:text-red-400 font-mono whitespace-pre-wrap">
          {error}
        </div>
      )}

      {result && <DataTable columns={result.columns} rows={result.rows} />}
    </div>
  );
}
