-- RFMセグメント値の許容リストテスト
-- 検証内容: dim_customer.rfm_segment が許容された値のいずれかであること
-- 許容値: 'チャンピオン', 'ロイヤル', '有望', 'リスクあり', '休眠', 'その他', '未購入'
-- 0行返却 = テスト合格

SELECT
  customer_key,
  rfm_segment
FROM {{ ref('dim_customer') }}
WHERE rfm_segment NOT IN ('チャンピオン', 'ロイヤル', '有望', 'リスクあり', '休眠', 'その他', '未購入')
