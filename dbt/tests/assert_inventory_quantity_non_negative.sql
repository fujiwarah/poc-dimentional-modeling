-- 在庫数の非負テスト
-- 検証内容: fact_inventory_daily.quantity_on_hand が 0 以上であること
-- 0行返却 = テスト合格

SELECT
  product_key,
  date_key,
  quantity_on_hand
FROM {{ ref('fact_inventory_daily') }}
WHERE quantity_on_hand < 0
