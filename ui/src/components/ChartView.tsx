import { useState, useMemo, useEffect, useRef } from "react";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { cn } from "../lib/cn.ts";
import { CHART_COLORS } from "../lib/chartColors.ts";

type ChartType = "bar" | "line" | "pie";

const CHART_TYPES: { key: ChartType; label: string }[] = [
  { key: "bar", label: "Bar" },
  { key: "line", label: "Line" },
  { key: "pie", label: "Pie" },
];

interface Props {
  columns: string[];
  rows: Record<string, string>[];
}

function isNumeric(val: string | undefined): boolean {
  if (val === undefined || val === null || val === "") return false;
  return !isNaN(Number(val));
}

function detectNumericColumns(columns: string[], rows: Record<string, string>[]): string[] {
  const sample = rows.slice(0, 20);
  return columns.filter((col) =>
    sample.length > 0 && sample.every((r) => r[col] === null || r[col] === undefined || isNumeric(r[col])),
  );
}

export default function ChartView({ columns, rows }: Props) {
  const numericCols = useMemo(() => detectNumericColumns(columns, rows), [columns, rows]);
  const categoryCols = useMemo(() => {
    const numSet = new Set(numericCols);
    return columns.filter((c) => !numSet.has(c));
  }, [columns, numericCols]);

  const [chartType, setChartType] = useState<ChartType>("bar");
  const [xCol, setXCol] = useState(() => categoryCols[0] ?? columns[0] ?? "");
  const [yCols, setYCols] = useState<string[]>(() => numericCols.slice(0, 2));

  const prevColsKeyRef = useRef("");
  useEffect(() => {
    const key = columns.join(",");
    if (key !== prevColsKeyRef.current) {
      prevColsKeyRef.current = key;
      setXCol(categoryCols[0] ?? columns[0] ?? "");
      setYCols(numericCols.slice(0, 2));
    }
  }, [columns, categoryCols, numericCols]);

  const chartData = useMemo(
    () =>
      rows.map((row) => {
        const entry: Record<string, string | number> = { [xCol]: row[xCol] ?? "" };
        for (const yc of yCols) {
          entry[yc] = Number(row[yc]) || 0;
        }
        return entry;
      }),
    [rows, xCol, yCols],
  );

  const pieData = useMemo(() => {
    if (chartType !== "pie" || yCols.length === 0) return [];
    const yc = yCols[0];
    return rows.slice(0, 20).map((row) => ({
      name: String(row[xCol] ?? ""),
      value: Number(row[yc]) || 0,
    }));
  }, [chartType, rows, xCol, yCols]);

  const toggleYCol = (col: string) => {
    setYCols((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col],
    );
  };

  if (columns.length < 2) {
    return (
      <div className="flex items-center justify-center h-64 text-sm text-zinc-400">
        チャート表示には2列以上のデータが必要です
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-3 text-xs">
        <div className="flex bg-zinc-100 dark:bg-zinc-900 rounded-md p-0.5">
          {CHART_TYPES.map((ct) => (
            <button
              key={ct.key}
              onClick={() => setChartType(ct.key)}
              className={cn(
                "px-3 py-1 rounded transition-colors",
                chartType === ct.key
                  ? "bg-white dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100 shadow-sm dark:shadow-none"
                  : "text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300",
              )}
            >
              {ct.label}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-1.5">
          <span className="text-zinc-400">X:</span>
          <select
            value={xCol}
            onChange={(e) => setXCol(e.target.value)}
            className="bg-white dark:bg-zinc-900 border rounded-md px-2 py-1 text-xs text-zinc-600 dark:text-zinc-400"
          >
            {columns.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-zinc-400">Y:</span>
          {numericCols.map((col) => (
            <button
              key={col}
              onClick={() => toggleYCol(col)}
              className={cn(
                "px-2 py-0.5 rounded-full text-[10px] font-medium transition-colors border",
                yCols.includes(col)
                  ? "bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 border-blue-200 dark:border-blue-800"
                  : "text-zinc-400 border-zinc-200 dark:border-zinc-700 hover:text-zinc-600 dark:hover:text-zinc-300",
              )}
            >
              {col}
            </button>
          ))}
          {numericCols.length === 0 && (
            <span className="text-zinc-400 italic">数値カラムが見つかりません</span>
          )}
        </div>
      </div>

      <div className="h-80 w-full">
        {yCols.length === 0 ? (
          <div className="flex items-center justify-center h-full text-sm text-zinc-400">
            Y軸のカラムを選択してください
          </div>
        ) : chartType === "pie" ? (
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={pieData}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                outerRadius={120}
                label={(props: { name?: string; percent?: number }) =>
                  `${props.name ?? ""}: ${((props.percent ?? 0) * 100).toFixed(0)}%`
                }
                labelLine
              >
                {pieData.map((entry, i) => (
                  <Cell key={`${entry.name}-${i}`} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        ) : chartType === "line" ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
              <XAxis dataKey={xCol} tick={{ fontSize: 10 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip />
              <Legend />
              {yCols.map((yc, i) => (
                <Line key={yc} type="monotone" dataKey={yc} stroke={CHART_COLORS[i % CHART_COLORS.length]} strokeWidth={2} dot={chartData.length <= 30} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
              <XAxis dataKey={xCol} tick={{ fontSize: 10 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip />
              <Legend />
              {yCols.map((yc, i) => (
                <Bar key={yc} dataKey={yc} fill={CHART_COLORS[i % CHART_COLORS.length]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
