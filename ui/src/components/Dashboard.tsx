import { useState } from "react";
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { type QueryResult } from "../lib/bq.ts";
import { cn } from "../lib/cn.ts";
import { CHART_COLORS } from "../lib/chartColors.ts";
import { useQuery } from "../hooks/useQuery.ts";

interface BaseCard {
  id: string;
  title: string;
  sql: string;
}

interface BarLineCard extends BaseCard {
  chart: "bar" | "line";
  xKey: string;
  yKeys: string[];
}

interface PieCard extends BaseCard {
  chart: "pie";
  labelKey: string;
  valueKey: string;
}

type CardConfig = BarLineCard | PieCard;

const CARDS: CardConfig[] = [
  {
    id: "monthly-sales",
    title: "月別売上推移",
    sql: `SELECT d.year_month, SUM(f.net_amount) AS sales, SUM(f.gross_profit) AS profit, COUNT(DISTINCT f.order_id) AS orders
FROM dwh.fact_sales f JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY 1 ORDER BY 1`,
    chart: "line",
    xKey: "year_month",
    yKeys: ["sales", "profit"],
  },
  {
    id: "category-sales",
    title: "カテゴリ別売上",
    sql: `SELECT p.parent_category_name AS category, SUM(f.net_amount) AS sales
FROM dwh.fact_sales f JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY 1 ORDER BY sales DESC`,
    chart: "bar",
    xKey: "category",
    yKeys: ["sales"],
  },
  {
    id: "rfm-segments",
    title: "RFMセグメント分布",
    sql: `SELECT rfm_segment, COUNT(*) AS customers
FROM dwh.dim_customer WHERE total_orders > 0
GROUP BY 1 ORDER BY customers DESC`,
    chart: "pie",
    labelKey: "rfm_segment",
    valueKey: "customers",
  },
  {
    id: "abc-analysis",
    title: "ABC分析（商品ランク別）",
    sql: `SELECT p.abc_rank, COUNT(*) AS products, SUM(f.net_amount) AS revenue
FROM dwh.fact_sales f JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY 1 ORDER BY 1`,
    chart: "bar",
    xKey: "abc_rank",
    yKeys: ["revenue", "products"],
  },
  {
    id: "funnel",
    title: "ファネル分析",
    sql: `SELECT page_type, COUNT(*) AS views
FROM dwh.fact_page_views
GROUP BY 1
ORDER BY CASE page_type WHEN 'home' THEN 1 WHEN 'category' THEN 2 WHEN 'product' THEN 3 WHEN 'cart' THEN 4 WHEN 'complete' THEN 5 ELSE 6 END`,
    chart: "bar",
    xKey: "page_type",
    yKeys: ["views"],
  },
  {
    id: "region-sales",
    title: "地域別売上",
    sql: `SELECT c.region, SUM(f.net_amount) AS sales, COUNT(DISTINCT c.customer_key) AS customers
FROM dwh.fact_sales f JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
GROUP BY 1 ORDER BY sales DESC`,
    chart: "bar",
    xKey: "region",
    yKeys: ["sales"],
  },
  {
    id: "hourly-pattern",
    title: "時間帯別注文パターン",
    sql: `SELECT t.hour_of_day, t.time_period, COUNT(DISTINCT f.order_id) AS orders, SUM(f.net_amount) AS sales
FROM dwh.fact_sales f JOIN dwh.dim_time t ON f.time_key = t.time_key
GROUP BY 1, 2 ORDER BY 1`,
    chart: "line",
    xKey: "hour_of_day",
    yKeys: ["orders"],
  },
  {
    id: "payment-mix",
    title: "決済手段構成",
    sql: `SELECT payment_method, COUNT(DISTINCT order_id) AS orders, SUM(net_amount) AS sales
FROM dwh.fact_sales GROUP BY 1 ORDER BY sales DESC`,
    chart: "pie",
    labelKey: "payment_method",
    valueKey: "sales",
  },
];

function toNumber(val: string | undefined): number {
  return Number(val) || 0;
}

function DashboardCard({ config }: { config: CardConfig }) {
  const { result, loading, error } = useQuery(config.sql);
  const [showSql, setShowSql] = useState(false);

  return (
    <div className="border rounded-lg bg-white dark:bg-zinc-900 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between px-3 py-2 border-b border-zinc-100 dark:border-zinc-800">
        <h3 className="text-xs font-semibold text-zinc-700 dark:text-zinc-300">
          {config.title}
        </h3>
        <button
          onClick={() => setShowSql((v) => !v)}
          className={cn(
            "text-[10px] px-1.5 py-0.5 rounded transition-colors",
            showSql
              ? "bg-blue-50 dark:bg-blue-900/20 text-blue-500"
              : "text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-300",
          )}
        >
          SQL
        </button>
      </div>

      {showSql && (
        <pre className="px-3 py-2 text-[10px] font-mono text-zinc-500 dark:text-zinc-400 bg-zinc-50 dark:bg-zinc-950 border-b border-zinc-100 dark:border-zinc-800 overflow-x-auto whitespace-pre-wrap">
          {config.sql}
        </pre>
      )}

      <div className="flex-1 p-3 min-h-[220px] flex items-center justify-center">
        {loading && (
          <span className="text-xs text-blue-500 dark:text-blue-400 animate-pulse">loading...</span>
        )}
        {error && (
          <span className="text-xs text-red-500 font-mono">{error}</span>
        )}
        {result && <CardChart config={config} result={result} />}
      </div>
    </div>
  );
}

function CardChart({ config, result }: { config: CardConfig; result: QueryResult }) {
  const data = result.rows;

  if (config.chart === "pie") {
    const pieData = data.map((r) => ({
      name: r[config.labelKey] ?? "",
      value: toNumber(r[config.valueKey]),
    }));
    return (
      <ResponsiveContainer width="100%" height={200}>
        <PieChart>
          <Pie
            data={pieData}
            dataKey="value"
            nameKey="name"
            cx="50%"
            cy="50%"
            outerRadius={75}
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
        </PieChart>
      </ResponsiveContainer>
    );
  }

  const chartData = data.map((r) => {
    const entry: Record<string, string | number> = { [config.xKey]: r[config.xKey] ?? "" };
    for (const yk of config.yKeys) {
      entry[yk] = toNumber(r[yk]);
    }
    return entry;
  });

  if (config.chart === "line") {
    return (
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
          <XAxis dataKey={config.xKey} tick={{ fontSize: 9 }} interval="preserveStartEnd" />
          <YAxis tick={{ fontSize: 9 }} />
          <Tooltip />
          <Legend />
          {config.yKeys.map((yk, i) => (
            <Line key={yk} type="monotone" dataKey={yk} stroke={CHART_COLORS[i]} strokeWidth={2} dot={chartData.length <= 30} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
        <XAxis dataKey={config.xKey} tick={{ fontSize: 9 }} interval={0} />
        <YAxis tick={{ fontSize: 9 }} />
        <Tooltip />
        <Legend />
        {config.yKeys.map((yk, i) => (
          <Bar key={yk} dataKey={yk} fill={CHART_COLORS[i]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

export default function Dashboard() {
  return (
    <div className="h-full overflow-y-auto">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 pb-4">
        {CARDS.map((card) => (
          <DashboardCard key={card.id} config={card} />
        ))}
      </div>
    </div>
  );
}
