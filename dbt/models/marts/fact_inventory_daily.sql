WITH inventory AS (
    SELECT * FROM {{ ref('int_inventory_with_sales') }}
),

dim_product AS (
    SELECT * FROM {{ ref('dim_product') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    dp.product_key,
    dd.date_key,
    inv.quantity_on_hand,
    inv.quantity_reserved,
    inv.quantity_available,
    inv.reorder_point,
    inv.quantity_available < inv.reorder_point AS is_below_reorder_point,
    inv.quantity_available <= 0 AS is_out_of_stock,
    inv.days_of_supply
FROM inventory inv
INNER JOIN dim_product dp ON inv.product_id = dp.product_id
INNER JOIN dim_date dd ON CAST(FORMAT_DATE('%Y%m%d', inv.snapshot_date) AS INT64) = dd.date_key
