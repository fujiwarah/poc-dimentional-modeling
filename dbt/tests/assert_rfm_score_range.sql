-- RFMスコアの範囲テスト
-- 検証内容:
--   (A) rfm_recency_score, rfm_frequency_score, rfm_monetary_score が NOT NULL の場合、1〜5 の範囲内であること
--   (B) total_orders = 0（注文なし）の顧客は、3つのスコアが全て NULL であること
-- 0行返却 = テスト合格

SELECT
  customer_key,
  total_orders,
  rfm_recency_score,
  rfm_frequency_score,
  rfm_monetary_score
FROM {{ ref('dim_customer') }}
WHERE
  -- (A) スコアが NOT NULL なのに 1〜5 の範囲外
  (rfm_recency_score IS NOT NULL AND (rfm_recency_score < 1 OR rfm_recency_score > 5))
  OR (rfm_frequency_score IS NOT NULL AND (rfm_frequency_score < 1 OR rfm_frequency_score > 5))
  OR (rfm_monetary_score IS NOT NULL AND (rfm_monetary_score < 1 OR rfm_monetary_score > 5))
  -- (B) 注文なし顧客なのにスコアが NULL でない
  OR (total_orders = 0 AND (rfm_recency_score IS NOT NULL OR rfm_frequency_score IS NOT NULL OR rfm_monetary_score IS NOT NULL))
