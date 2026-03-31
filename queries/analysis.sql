-- =============================================
-- 分析クエリ 1: 月別売上推移
-- 目的: 月ごとの売上合計・注文数・注文明細数の時系列推移を確認する
-- 使用テーブル: dwh.fact_sales (ファクト), dwh.dim_date (ディメンション)
-- JOIN 方式: fact_sales.date_key = dim_date.date_key (サロゲートキー経由)
-- 集計軸: year, month
-- 指標: 売上合計 (net_amount), ユニーク注文数 (DISTINCT order_id), 注文明細数
-- =============================================
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS line_item_count,
    SUM(f.net_amount) AS total_net_amount,
    SUM(f.quantity) AS total_quantity
FROM dwh.fact_sales f
INNER JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- =============================================
-- 分析クエリ 2: 商品カテゴリ別売上ランキング
-- 目的: 親カテゴリ別の売上合計・販売数量・商品数を集計しランキングする
-- 使用テーブル: dwh.fact_sales (ファクト), dwh.dim_product (ディメンション)
-- JOIN 方式: fact_sales.product_key = dim_product.product_key (サロゲートキー経由)
-- 集計軸: parent_category_name
-- 指標: 売上合計, 販売数量, ユニーク商品数
-- =============================================
SELECT
    p.parent_category_name,
    SUM(f.net_amount) AS total_net_amount,
    SUM(f.quantity) AS total_quantity,
    COUNT(DISTINCT p.product_id) AS product_count
FROM dwh.fact_sales f
INNER JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY p.parent_category_name
ORDER BY total_net_amount DESC;

-- =============================================
-- 分析クエリ 3: 地方別・顧客セグメント分析
-- 目的: 地方 (region) 別の顧客数・平均注文金額・注文頻度を分析する
-- 使用テーブル: dwh.fact_sales (ファクト), dwh.dim_customer (ディメンション)
-- JOIN 方式: fact_sales.customer_key = dim_customer.customer_key (サロゲートキー経由)
-- 集計軸: region
-- 指標: 顧客数, 注文数, 平均注文金額, 注文頻度 (注文数/顧客数)
-- =============================================
SELECT
    c.region,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    COUNT(DISTINCT f.order_id) AS order_count,
    ROUND(AVG(f.net_amount), 2) AS avg_order_amount,
    ROUND(CAST(COUNT(DISTINCT f.order_id) AS FLOAT64) / COUNT(DISTINCT c.customer_key), 2) AS orders_per_customer
FROM dwh.fact_sales f
INNER JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.region
ORDER BY customer_count DESC;

-- =============================================
-- 分析クエリ 4: 注文ステータス分析
-- 目的: ステータス別 (日本語ラベル) の件数・売上合計・平均単価を分析する
-- 使用テーブル: dwh.fact_sales (ファクト), dwh.dim_order_status (ディメンション)
-- JOIN 方式: fact_sales.order_status_key = dim_order_status.order_status_key (サロゲートキー経由)
-- 集計軸: status_label (日本語), status_code
-- 指標: 注文明細件数, 売上合計, 平均単価
-- =============================================
SELECT
    s.status_label,
    s.status_code,
    COUNT(*) AS item_count,
    SUM(f.net_amount) AS total_net_amount,
    ROUND(AVG(f.net_amount), 2) AS avg_net_amount
FROM dwh.fact_sales f
INNER JOIN dwh.dim_order_status s ON f.order_status_key = s.order_status_key
GROUP BY s.status_label, s.status_code
ORDER BY total_net_amount DESC;

-- =============================================
-- 分析クエリ 5: 曜日別注文傾向
-- 目的: 曜日ごとの注文件数・売上合計・平均注文金額を分析しパターンを把握する
-- 使用テーブル: dwh.fact_sales (ファクト), dwh.dim_date (ディメンション)
-- JOIN 方式: fact_sales.date_key = dim_date.date_key (サロゲートキー経由)
-- 集計軸: day_of_week, day_name
-- 指標: ユニーク注文数, 売上合計, 平均注文金額
-- day_of_week: 0=月曜 ~ 6=日曜 の順でソート
-- =============================================
SELECT
    d.day_of_week,
    d.day_name,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.net_amount) AS total_net_amount,
    ROUND(AVG(f.net_amount), 2) AS avg_net_amount
FROM dwh.fact_sales f
INNER JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY d.day_of_week, d.day_name
ORDER BY d.day_of_week;
