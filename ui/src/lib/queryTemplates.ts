export interface QueryTemplate {
  label: string;
  category: string;
  sql: string;
}

export const QUERY_TEMPLATES: QueryTemplate[] = [
  {
    label: "月別売上推移",
    category: "売上分析",
    sql: `SELECT
  d.year, d.month, d.month_name,
  COUNT(DISTINCT f.order_id) AS order_count,
  SUM(f.net_amount) AS total_sales,
  SUM(f.gross_profit) AS total_profit
FROM dwh.fact_sales f
JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY 1, 2, 3
ORDER BY 1, 2`,
  },
  {
    label: "カテゴリ別売上",
    category: "売上分析",
    sql: `SELECT
  p.parent_category_name,
  p.category_name,
  COUNT(DISTINCT f.order_id) AS order_count,
  SUM(f.net_amount) AS total_sales,
  SUM(f.gross_profit) AS total_profit,
  ROUND(AVG(f.discount_rate), 1) AS avg_discount_rate
FROM dwh.fact_sales f
JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY 1, 2
ORDER BY total_sales DESC`,
  },
  {
    label: "RFMセグメント分布",
    category: "顧客分析",
    sql: `SELECT
  rfm_segment,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_spent), 0) AS avg_spent,
  ROUND(AVG(ltv_estimated), 0) AS avg_ltv
FROM dwh.dim_customer
WHERE total_orders > 0
GROUP BY 1
ORDER BY avg_spent DESC`,
  },
  {
    label: "顧客LTV上位20",
    category: "顧客分析",
    sql: `SELECT
  full_name, prefecture, region,
  rfm_segment, total_orders, total_spent,
  ROUND(ltv_estimated, 0) AS ltv_estimated
FROM dwh.dim_customer
WHERE ltv_estimated IS NOT NULL
ORDER BY ltv_estimated DESC
LIMIT 20`,
  },
  {
    label: "ABC分析（商品ランク）",
    category: "商品分析",
    sql: `SELECT
  abc_rank,
  COUNT(*) AS product_count,
  SUM(s.total_sales) AS total_revenue,
  ROUND(AVG(p.gross_margin_pct), 1) AS avg_margin_pct
FROM dwh.dim_product p
JOIN (
  SELECT product_key, SUM(net_amount) AS total_sales
  FROM dwh.fact_sales GROUP BY 1
) s ON p.product_key = s.product_key
GROUP BY 1
ORDER BY 1`,
  },
  {
    label: "売上Top20商品",
    category: "商品分析",
    sql: `SELECT
  p.product_name, p.category_name, p.abc_rank,
  SUM(f.quantity) AS total_qty,
  SUM(f.net_amount) AS total_sales,
  SUM(f.gross_profit) AS total_profit
FROM dwh.fact_sales f
JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY 1, 2, 3
ORDER BY total_sales DESC
LIMIT 20`,
  },
  {
    label: "ページタイプ別PV",
    category: "トラフィック",
    sql: `SELECT
  page_type,
  COUNT(*) AS page_views,
  COUNT(DISTINCT session_id) AS sessions,
  ROUND(AVG(duration_seconds), 1) AS avg_duration_sec
FROM dwh.fact_page_views
GROUP BY 1
ORDER BY page_views DESC`,
  },
  {
    label: "セッションコンバージョン",
    category: "トラフィック",
    sql: `SELECT
  referrer_type,
  COUNT(*) AS sessions,
  COUNTIF(has_cart) AS cart_sessions,
  COUNTIF(has_purchase) AS purchase_sessions,
  ROUND(COUNTIF(has_purchase) * 100.0 / COUNT(*), 1) AS cvr_pct
FROM dwh.int_page_view_sessions
GROUP BY 1
ORDER BY sessions DESC`,
  },
  {
    label: "地域別売上",
    category: "売上分析",
    sql: `SELECT
  c.region,
  COUNT(DISTINCT c.customer_key) AS customers,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(f.net_amount) AS total_sales
FROM dwh.fact_sales f
JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
GROUP BY 1
ORDER BY total_sales DESC`,
  },
  {
    label: "在庫欠品状況",
    category: "在庫",
    sql: `SELECT
  p.product_name, p.category_name,
  fi.quantity_on_hand, fi.quantity_available,
  fi.reorder_point, fi.days_of_supply,
  fi.is_out_of_stock, fi.is_below_reorder_point
FROM dwh.fact_inventory_daily fi
JOIN dwh.dim_product p ON fi.product_key = p.product_key
JOIN dwh.dim_date d ON fi.date_key = d.date_key
WHERE d.full_date = (SELECT MAX(full_date) FROM dwh.dim_date dd
  JOIN dwh.fact_inventory_daily fi2 ON dd.date_key = fi2.date_key)
ORDER BY fi.days_of_supply ASC
LIMIT 20`,
  },
  {
    label: "時間帯別注文パターン",
    category: "売上分析",
    sql: `SELECT
  t.hour_of_day,
  t.time_period,
  COUNT(DISTINCT f.order_id) AS order_count,
  SUM(f.net_amount) AS total_sales
FROM dwh.fact_sales f
JOIN dwh.dim_time t ON f.time_key = t.time_key
GROUP BY 1, 2
ORDER BY 1`,
  },
];
