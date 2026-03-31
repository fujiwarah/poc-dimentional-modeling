import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import {
  listTables,
  getSchema,
  previewTablePage,
  type TableRef,
  type SchemaField,
} from "../lib/bq.ts";
import { cn } from "../lib/cn.ts";
import DataTable from "./DataTable.tsx";
import { usePaginatedQuery } from "../hooks/usePaginatedQuery.ts";

const DATASETS = ["dwh", "raw"] as const;

const PREFIX_TO_GROUP: Record<string, string> = {
  stg: "staging",
  int: "intermediate",
  dim: "marts",
  fact: "marts",
};

const GROUP_ORDER = ["staging", "intermediate", "marts", "other"];

interface TableView {
  selected: string | null;
  schema: SchemaField[];
  tab: "schema" | "data";
  pageSize: number;
}

const INITIAL_VIEW: TableView = {
  selected: null,
  schema: [],
  tab: "data",
  pageSize: 100,
};

export default function TableBrowser() {
  const [dataset, setDataset] = useState("dwh");
  const [tables, setTables] = useState<TableRef[]>([]);
  const [view, setView] = useState<TableView>(INITIAL_VIEW);
  const [error, setError] = useState<string | null>(null);
  const query = usePaginatedQuery();

  const selectedRef = useRef(view.selected);
  selectedRef.current = view.selected;

  // useEffect required: fetch table list from BigQuery API on dataset change
  useEffect(() => {
    setError(null);
    listTables(dataset)
      .then(setTables)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [dataset]);

  const switchDataset = useCallback(
    (ds: string) => {
      setDataset(ds);
      setTables([]);
      setView((v) => ({ ...v, selected: null, schema: [] }));
      setError(null);
      query.reset();
    },
    [query.reset],
  );

  const selectTable = useCallback(
    (tableId: string) => {
      setView((v) => ({ ...v, selected: tableId, schema: [] }));
      setError(null);
      getSchema(dataset, tableId)
        .then((s) => setView((v) => ({ ...v, schema: s })))
        .catch((e) => setError(e instanceof Error ? e.message : String(e)));
      query.execute((page, knownTotal) =>
        previewTablePage(dataset, tableId, page, view.pageSize, knownTotal),
      );
    },
    [dataset, view.pageSize, query.execute],
  );

  const changePageSize = useCallback(
    (newSize: number) => {
      setView((v) => ({ ...v, pageSize: newSize }));
      const tableId = selectedRef.current;
      if (!tableId) return;
      query.execute((page, knownTotal) =>
        previewTablePage(dataset, tableId, page, newSize, knownTotal),
      );
    },
    [dataset, query.execute],
  );

  const groupedTables = useMemo(
    () =>
      tables.reduce<Record<string, TableRef[]>>((acc, t) => {
        const prefix = t.tableId.split("_")[0];
        const group = PREFIX_TO_GROUP[prefix] ?? "other";
        (acc[group] ??= []).push(t);
        return acc;
      }, {}),
    [tables],
  );

  const schemaAsRows = useMemo(
    () =>
      view.schema.map((f) => ({
        Column: f.name,
        Type: f.type,
        Mode: f.mode,
      })),
    [view.schema],
  );

  const displayError = error || query.error;

  return (
    <div className="flex gap-4 h-full">
      <div className="w-56 shrink-0 flex flex-col gap-3">
        <div className="flex bg-zinc-100 dark:bg-zinc-900 rounded-lg p-0.5">
          {DATASETS.map((ds) => (
            <button
              key={ds}
              onClick={() => switchDataset(ds)}
              className={cn(
                "flex-1 px-3 py-1.5 rounded-md text-xs font-medium transition-colors",
                dataset === ds
                  ? "bg-white dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100 shadow-sm dark:shadow-none"
                  : "text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300",
              )}
            >
              {ds}
            </button>
          ))}
        </div>

        <div className="flex flex-col gap-3 overflow-y-auto">
          {GROUP_ORDER.map((group) =>
            groupedTables[group] ? (
              <div key={group}>
                <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-400 dark:text-zinc-500">
                  {group}
                </div>
                {groupedTables[group].map((t) => (
                  <button
                    key={t.tableId}
                    onClick={() => selectTable(t.tableId)}
                    className={cn(
                      "w-full text-left px-2 py-1 rounded text-xs font-mono transition-colors truncate",
                      view.selected === t.tableId
                        ? "bg-blue-100 dark:bg-blue-600/20 text-blue-600 dark:text-blue-400"
                        : "text-zinc-600 dark:text-zinc-400 hover:bg-zinc-100 dark:hover:bg-zinc-800 hover:text-zinc-900 dark:hover:text-zinc-200",
                    )}
                  >
                    {t.tableId}
                  </button>
                ))}
              </div>
            ) : null,
          )}
        </div>
      </div>

      <div className="flex-1 flex flex-col gap-3 min-w-0">
        {displayError && (
          <div className="bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-900/50 rounded-lg p-3 text-sm text-red-600 dark:text-red-400 font-mono">
            {displayError}
          </div>
        )}

        {view.selected ? (
          <>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <h2 className="font-mono text-sm font-medium text-zinc-800 dark:text-zinc-200">
                  {dataset}.{view.selected}
                </h2>
                {query.result && (
                  <span className="text-xs text-zinc-400 dark:text-zinc-600">
                    {query.result.totalRows} rows
                  </span>
                )}
                {query.loading && (
                  <span className="text-xs text-blue-500 dark:text-blue-400 animate-pulse">
                    loading...
                  </span>
                )}
              </div>

              <div className="flex items-center gap-3">
                <div className="flex bg-zinc-100 dark:bg-zinc-900 rounded-md p-0.5 text-xs">
                  {(["data", "schema"] as const).map((t) => (
                    <button
                      key={t}
                      onClick={() => setView((v) => ({ ...v, tab: t }))}
                      className={cn(
                        "px-3 py-1 rounded transition-colors capitalize",
                        view.tab === t
                          ? "bg-white dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100 shadow-sm dark:shadow-none"
                          : "text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300",
                      )}
                    >
                      {t}
                    </button>
                  ))}
                </div>

                {view.tab === "data" && (
                  <select
                    value={view.pageSize}
                    onChange={(e) => changePageSize(Number(e.target.value))}
                    className="bg-white dark:bg-zinc-900 border rounded-md px-2 py-1 text-xs text-zinc-600 dark:text-zinc-400"
                  >
                    {[50, 100, 200, 500].map((n) => (
                      <option key={n} value={n}>
                        {n} rows/page
                      </option>
                    ))}
                  </select>
                )}
              </div>
            </div>

            {view.tab === "schema" && (
              <DataTable
                columns={["Column", "Type", "Mode"]}
                rows={schemaAsRows}
              />
            )}

            {view.tab === "data" && query.result && (
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
              />
            )}
          </>
        ) : (
          <div className="flex items-center justify-center h-full text-zinc-400 dark:text-zinc-600 text-sm">
            ← テーブルを選択してください
          </div>
        )}
      </div>
    </div>
  );
}
