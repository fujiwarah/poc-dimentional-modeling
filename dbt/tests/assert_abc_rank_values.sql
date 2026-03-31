-- ABCランクの値テスト
-- 検証内容: dim_product.abc_rank が 'A', 'B', 'C' のいずれかであること
-- 0行返却 = テスト合格

SELECT
  product_key,
  product_name,
  abc_rank
FROM {{ ref('dim_product') }}
WHERE abc_rank NOT IN ('A', 'B', 'C')
