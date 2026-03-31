-- 在庫利用可能数の整合性テスト
-- 検証内容: fact_inventory_daily.quantity_available = quantity_on_hand - quantity_reserved であること
-- 0行返却 = テスト合格

SELECT
  product_key,
  date_key,
  quantity_on_hand,
  quantity_reserved,
  quantity_available,
  (quantity_on_hand - quantity_reserved) AS expected_available
FROM {{ ref('fact_inventory_daily') }}
WHERE quantity_available != (quantity_on_hand - quantity_reserved)
