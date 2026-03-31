import { useState, useEffect, useCallback } from "react";
import {
  listTables,
  getSchema,
  runQuery,
  type TableRef,
  type SchemaField,
  type QueryResult,
} from "../lib/bq.ts";
import DataTable from "./DataTable.tsx";

const DATASETS = ["dwh", "raw"] as const;

function classNames(...classes: (string | false | undefined)[]) {
  return classes.filter(Boolean).join(" ");
}

export default function TableBrowser() {
  const [dataset, setDataset] = useState<string>("dwh");
  const [tables, setTables] = useState<TableRef[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [schema, setSchema] = useState<SchemaField[]>([]);
  const [data, setData] = useState<QueryResult | null>(null);
  const [tab, setTab] = useState<"schema" | "data">("data");
  const [limit, setLimit] = useState(100);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    listTables(dataset).then((t) => {
      setTables(t);
      setSelected(null);
      setSchema([]);
      setData(null);
    });
  }, [dataset]);

  const selectTable = useCallback(
    async (tableId: string) => {
      setSelected(tableId);
      setLoading(true);
      try {
        const [s, d] = await Promise.all([
          getSchema(dataset, tableId),
          runQuery(
            `SELECT * FROM \`poc-project.${dataset}.${tableId}\` LIMIT ${limit}`,
          ),
        ]);
        setSchema(s);
        setData(d);
      } finally {
        setLoading(false);
      }
    },
    [dataset, limit],
  );

  const reload = useCallback(async () => {
    if (!selected) return;
    setLoading(true);
    try {
      const d = await runQuery(
        `SELECT * FROM \`poc-project.${dataset}.${selected}\` LIMIT ${limit}`,
      );
      setData(d);
    } finally {
      setLoading(false);
    }
  }, [dataset, selected, limit]);

  const groupedTables = tables.reduce<Record<string, TableRef[]>>(
    (acc, t) => {
      const prefix = t.tableId.split("_")[0];
      const group =
        prefix === "stg"
          ? "staging"
          : prefix === "int"
            ? "intermediate"
            : prefix === "dim" || prefix === "fact"
              ? "marts"
              : "other";
      (acc[group] ??= []).push(t);
      return acc;
    },
    {},
  );

  const groupOrder = ["staging", "intermediate", "marts", "other"];

  return (
    <div className="flex gap-4 h-full">
      {/* Sidebar */}
      <div className="w-56 shrink-0 flex flex-col gap-3">
        {/* Dataset selector */}
        <div className="flex bg-zinc-900 rounded-lg p-0.5">
          {DATASETS.map((ds) => (
            <button
              key={ds}
              onClick={() => setDataset(ds)}
              className={classNames(
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

        {/* Table list */}
        <div className="flex flex-col gap-3 overflow-y-auto">
          {groupOrder.map((group) =>
            groupedTables[group] ? (
              <div key={group}>
                <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-600">
                  {group}
                </div>
                {groupedTables[group].map((t) => (
                  <button
                    key={t.tableId}
                    onClick={() => selectTable(t.tableId)}
                    className={classNames(
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

      {/* Main area */}
      <div className="flex-1 flex flex-col gap-3 min-w-0">
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
                {/* Tab switch */}
                <div className="flex bg-zinc-900 rounded-md p-0.5 text-xs">
                  {(["data", "schema"] as const).map((t) => (
                    <button
                      key={t}
                      onClick={() => setTab(t)}
                      className={classNames(
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
                    onChange={(e) => {
                      setLimit(Number(e.target.value));
                      setTimeout(reload, 0);
                    }}
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
              <div className="overflow-auto rounded-lg border border-zinc-800">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-zinc-800 bg-zinc-900">
                      <th className="px-3 py-2 text-left text-xs font-medium text-zinc-400">
                        Column
                      </th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-zinc-400">
                        Type
                      </th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-zinc-400">
                        Mode
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {schema.map((f) => (
                      <tr
                        key={f.name}
                        className="border-b border-zinc-800/50 hover:bg-zinc-900/50"
                      >
                        <td className="px-3 py-1.5 font-mono text-xs text-zinc-200">
                          {f.name}
                        </td>
                        <td className="px-3 py-1.5 font-mono text-xs text-amber-400/80">
                          {f.type}
                        </td>
                        <td className="px-3 py-1.5 font-mono text-xs text-zinc-500">
                          {f.mode}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
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
