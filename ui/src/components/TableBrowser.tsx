import { useState, useEffect, useCallback, useMemo } from "react";
import {
  listTables,
  getSchema,
  previewTable,
  type TableRef,
  type SchemaField,
  type QueryResult,
} from "../lib/bq.ts";
import { cn } from "../lib/cn.ts";
import DataTable from "./DataTable.tsx";

const DATASETS = ["dwh", "raw"] as const;

const PREFIX_TO_GROUP: Record<string, string> = {
  stg: "staging",
  int: "intermediate",
  dim: "marts",
  fact: "marts",
};

const GROUP_ORDER = ["staging", "intermediate", "marts", "other"];

export default function TableBrowser() {
  const [dataset, setDataset] = useState<string>("dwh");
  const [tables, setTables] = useState<TableRef[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [schema, setSchema] = useState<SchemaField[]>([]);
  const [data, setData] = useState<QueryResult | null>(null);
  const [tab, setTab] = useState<"schema" | "data">("data");
  const [limit, setLimit] = useState(100);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setError(null);
    listTables(dataset)
      .then((t) => {
        setTables(t);
        setSelected(null);
        setSchema([]);
        setData(null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [dataset]);

  const selectTable = useCallback(
    async (tableId: string) => {
      setSelected(tableId);
      setLoading(true);
      setError(null);
      try {
        const [s, d] = await Promise.all([
          getSchema(dataset, tableId),
          previewTable(dataset, tableId, limit),
        ]);
        setSchema(s);
        setData(d);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    },
    [dataset, limit],
  );

  useEffect(() => {
    if (!selected) return;
    let cancelled = false;
    setLoading(true);
    previewTable(dataset, selected, limit)
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [dataset, selected, limit]);

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
      schema.map((f) => ({
        Column: f.name,
        Type: f.type,
        Mode: f.mode,
      })),
    [schema],
  );

  return (
    <div className="flex gap-4 h-full">
      <div className="w-56 shrink-0 flex flex-col gap-3">
        <div className="flex bg-zinc-900 rounded-lg p-0.5">
          {DATASETS.map((ds) => (
            <button
              key={ds}
              onClick={() => setDataset(ds)}
              className={cn(
                "flex-1 px-3 py-1.5 rounded-md text-xs font-medium transition-colors",
                dataset === ds
                  ? "bg-zinc-700 text-zinc-100"
                  : "text-zinc-500 hover:text-zinc-300",
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
                <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-600">
                  {group}
                </div>
                {groupedTables[group].map((t) => (
                  <button
                    key={t.tableId}
                    onClick={() => selectTable(t.tableId)}
                    className={cn(
                      "w-full text-left px-2 py-1 rounded text-xs font-mono transition-colors truncate",
                      selected === t.tableId
                        ? "bg-blue-600/20 text-blue-400"
                        : "text-zinc-400 hover:bg-zinc-800 hover:text-zinc-200",
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
        {error && (
          <div className="bg-red-950/50 border border-red-900/50 rounded-lg p-3 text-sm text-red-400 font-mono">
            {error}
          </div>
        )}

        {selected ? (
          <>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <h2 className="font-mono text-sm font-medium text-zinc-200">
                  {dataset}.{selected}
                </h2>
                {data && (
                  <span className="text-xs text-zinc-600">
                    {data.totalRows} rows
                  </span>
                )}
                {loading && (
                  <span className="text-xs text-blue-400 animate-pulse">
                    loading...
                  </span>
                )}
              </div>

              <div className="flex items-center gap-3">
                <div className="flex bg-zinc-900 rounded-md p-0.5 text-xs">
                  {(["data", "schema"] as const).map((t) => (
                    <button
                      key={t}
                      onClick={() => setTab(t)}
                      className={cn(
                        "px-3 py-1 rounded transition-colors capitalize",
                        tab === t
                          ? "bg-zinc-700 text-zinc-100"
                          : "text-zinc-500 hover:text-zinc-300",
                      )}
                    >
                      {t}
                    </button>
                  ))}
                </div>

                {tab === "data" && (
                  <select
                    value={limit}
                    onChange={(e) => setLimit(Number(e.target.value))}
                    className="bg-zinc-900 border border-zinc-800 rounded-md px-2 py-1 text-xs text-zinc-400"
                  >
                    {[50, 100, 200, 500].map((n) => (
                      <option key={n} value={n}>
                        {n} rows
                      </option>
                    ))}
                  </select>
                )}
              </div>
            </div>

            {tab === "schema" && (
              <DataTable
                columns={["Column", "Type", "Mode"]}
                rows={schemaAsRows}
              />
            )}

            {tab === "data" && data && (
              <DataTable columns={data.columns} rows={data.rows} />
            )}
          </>
        ) : (
          <div className="flex items-center justify-center h-full text-zinc-600 text-sm">
            ← テーブルを選択してください
          </div>
        )}
      </div>
    </div>
  );
}
