import { useState, useCallback, useRef } from "react";
import { runQueryPage, type SortOrder } from "../lib/bq.ts";
import { cn } from "../lib/cn.ts";
import DataTable from "./DataTable.tsx";
import ChartView from "./ChartView.tsx";
import SqlEditor from "./SqlEditor.tsx";
import { usePaginatedQuery } from "../hooks/usePaginatedQuery.ts";
import { useQueryHistory } from "../hooks/useQueryHistory.ts";
import { QUERY_TEMPLATES } from "../lib/queryTemplates.ts";

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

const PAGE_SIZE = 100;

const TEMPLATE_CATEGORIES = Array.from(new Set(QUERY_TEMPLATES.map((t) => t.category)));
const TEMPLATES_BY_CATEGORY = new Map(
  TEMPLATE_CATEGORIES.map((cat) => [cat, QUERY_TEMPLATES.filter((t) => t.category === cat)]),
);

type ResultView = "table" | "chart";

export default function QueryEditor() {
  const [sql, setSql] = useState(SAMPLE_QUERY);
  const [sort, setSort] = useState<SortOrder | null>(null);
  const [resultView, setResultView] = useState<ResultView>("table");
  const [showTemplates, setShowTemplates] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const executedSqlRef = useRef("");
  const query = usePaginatedQuery();
  const history = useQueryHistory();

  const execute = useCallback(() => {
    const snapshot = sql;
    executedSqlRef.current = snapshot;
    setSort(null);
    history.add(snapshot);
    query.execute((page, knownTotal) =>
      runQueryPage(snapshot, page, PAGE_SIZE, knownTotal),
    );
  }, [sql, query.execute, history.add]);

  const handleSortChange = useCallback(
    (newSort: SortOrder | null) => {
      setSort(newSort);
      const snapshot = executedSqlRef.current;
      query.execute(
        (page, knownTotal) =>
          runQueryPage(snapshot, page, PAGE_SIZE, knownTotal, newSort ?? undefined),
        true,
      );
    },
    [query.execute],
  );

  return (
    <div className="flex flex-col gap-3 h-full">
      <div className="flex items-center gap-2 text-xs">
        <button
          onClick={() => { setShowTemplates((v) => !v); setShowHistory(false); }}
          className={cn(
            "px-2.5 py-1 rounded-md border transition-colors",
            showTemplates
              ? "bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800 text-blue-600 dark:text-blue-400"
              : "border-zinc-200 dark:border-zinc-700 text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300",
          )}
        >
          Templates
        </button>
        <button
          onClick={() => { setShowHistory((v) => !v); setShowTemplates(false); }}
          className={cn(
            "px-2.5 py-1 rounded-md border transition-colors",
            showHistory
              ? "bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800 text-blue-600 dark:text-blue-400"
              : "border-zinc-200 dark:border-zinc-700 text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300",
          )}
        >
          History ({history.entries.length})
        </button>
      </div>

      {showTemplates && (
        <div className="border rounded-lg bg-white dark:bg-zinc-900 max-h-56 overflow-y-auto">
          {TEMPLATE_CATEGORIES.map((cat) => (
            <div key={cat}>
              <div className="px-3 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-zinc-400 dark:text-zinc-500 bg-zinc-50 dark:bg-zinc-900 sticky top-0">
                {cat}
              </div>
              {TEMPLATES_BY_CATEGORY.get(cat)!.map((t) => (
                <button
                  key={t.label}
                  onClick={() => { setSql(t.sql); setShowTemplates(false); }}
                  className="w-full text-left px-3 py-1.5 text-xs text-zinc-600 dark:text-zinc-400 hover:bg-zinc-50 dark:hover:bg-zinc-800 transition-colors"
                >
                  {t.label}
                </button>
              ))}
            </div>
          ))}
        </div>
      )}

      {showHistory && (
        <div className="border rounded-lg bg-white dark:bg-zinc-900 max-h-56 overflow-y-auto">
          {history.entries.length === 0 ? (
            <div className="p-3 text-xs text-zinc-400">履歴がありません</div>
          ) : (
            <>
              {history.entries.map((entry) => (
                <button
                  key={entry.timestamp}
                  onClick={() => { setSql(entry.sql); setShowHistory(false); }}
                  className="w-full text-left px-3 py-1.5 text-xs font-mono text-zinc-600 dark:text-zinc-400 hover:bg-zinc-50 dark:hover:bg-zinc-800 transition-colors flex items-center gap-2 border-b border-zinc-100 dark:border-zinc-800 last:border-b-0"
                >
                  <span className="truncate flex-1">{entry.sql.replace(/\s+/g, " ")}</span>
                  <span className="text-[10px] text-zinc-400 dark:text-zinc-600 shrink-0">
                    {new Date(entry.timestamp).toLocaleTimeString("ja-JP", { hour: "2-digit", minute: "2-digit" })}
                  </span>
                </button>
              ))}
              <button
                onClick={history.clear}
                className="w-full text-center py-1.5 text-[10px] text-zinc-400 hover:text-red-500 transition-colors"
              >
                履歴をクリア
              </button>
            </>
          )}
        </div>
      )}

      <SqlEditor value={sql} onChange={setSql} onRun={execute} />

      <div className="flex items-center gap-3">
        <button
          onClick={execute}
          disabled={query.loading || !sql.trim()}
          className="px-4 py-1.5 bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:hover:bg-blue-600 rounded-md text-sm font-medium text-white transition-colors"
        >
          {query.loading ? "Running..." : "Run"}
        </button>
        {query.result && (
          <>
            <span className="text-xs text-zinc-500 dark:text-zinc-400">
              {query.result.totalRows} rows
            </span>
            <div className="flex bg-zinc-100 dark:bg-zinc-900 rounded-md p-0.5 text-xs ml-auto">
              {(["table", "chart"] as const).map((v) => (
                <button
                  key={v}
                  onClick={() => setResultView(v)}
                  className={cn(
                    "px-3 py-1 rounded transition-colors",
                    resultView === v
                      ? "bg-white dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100 shadow-sm dark:shadow-none"
                      : "text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300",
                  )}
                >
                  {v === "table" ? "Table" : "Chart"}
                </button>
              ))}
            </div>
          </>
        )}
      </div>

      {query.error && (
        <div className="bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-900/50 rounded-lg p-3 text-sm text-red-600 dark:text-red-400 font-mono whitespace-pre-wrap">
          {query.error}
        </div>
      )}

      {query.result && resultView === "table" && (
        <DataTable
          columns={query.result.columns}
          rows={query.result.rows}
          pagination={{
            page: query.result.page,
            pageSize: query.result.pageSize,
            totalRows: query.result.totalRows,
            totalPages: query.result.totalPages,
            onPageChange: query.changePage,
          }}
          sorting={sort}
          onSortChange={handleSortChange}
        />
      )}

      {query.result && resultView === "chart" && (
        <ChartView
          columns={query.result.columns}
          rows={query.result.rows}
        />
      )}
    </div>
  );
}
