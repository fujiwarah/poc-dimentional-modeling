WITH inventory AS (
    SELECT * FROM {{ ref('stg_inventory_snapshots') }}
),

order_items AS (
    SELECT * FROM {{ ref('int_orders_with_items') }}
    WHERE status != 'cancelled'
),

-- Daily sales quantity per product
daily_sales AS (
    SELECT
        product_id,
        order_date AS sales_date,
        SUM(quantity) AS daily_quantity_sold
    FROM order_items
    GROUP BY product_id, order_date
),

-- Join inventory with daily sales
inventory_with_sales AS (
    SELECT
        inv.snapshot_id,
        inv.product_id,
        inv.snapshot_date,
        inv.quantity_on_hand,
        inv.quantity_reserved,
        inv.quantity_on_hand - inv.quantity_reserved AS quantity_available,
        inv.reorder_point,
        inv.lead_time_days,
        COALESCE(ds.daily_quantity_sold, 0) AS daily_quantity_sold
    FROM inventory inv
    LEFT JOIN daily_sales ds
        ON inv.product_id = ds.product_id
        AND inv.snapshot_date = ds.sales_date
),

-- 商品ごとの平均日次販売数（全期間）
product_avg_sales AS (
    SELECT
        product_id,
        AVG(CAST(daily_quantity_sold AS FLOAT64)) AS daily_avg_sales
    FROM (
        SELECT product_id, order_date AS sales_date, SUM(quantity) AS daily_quantity_sold
        FROM {{ ref('int_orders_with_items') }}
        WHERE status != 'cancelled'
        GROUP BY product_id, order_date
    )
    GROUP BY product_id
)

SELECT
    iws.snapshot_id,
    iws.product_id,
    iws.snapshot_date,
    iws.quantity_on_hand,
    iws.quantity_reserved,
    iws.quantity_available,
    iws.reorder_point,
    iws.lead_time_days,
    COALESCE(pas.daily_avg_sales, 0) AS daily_avg_sales,
    iws.quantity_available / NULLIF(pas.daily_avg_sales, 0) AS days_of_supply
FROM inventory_with_sales iws
LEFT JOIN product_avg_sales pas ON iws.product_id = pas.product_id
