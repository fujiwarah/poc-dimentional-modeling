-- fact_sales の net_amount が 0 以上であることを検証する。
-- 0 件を返せば PASS、1 件以上返せば FAIL。
SELECT
    order_item_id,
    net_amount
FROM {{ ref('fact_sales') }}
WHERE net_amount < 0
