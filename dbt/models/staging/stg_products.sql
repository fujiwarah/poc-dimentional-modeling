SELECT
    CAST(product_id AS INT64) AS product_id,
    CAST(product_name AS STRING) AS product_name,
    CAST(category_id AS INT64) AS category_id,
    CAST(unit_price AS NUMERIC) AS unit_price,
    CAST(description AS STRING) AS description,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(cost_price AS NUMERIC) AS cost_price,
    CAST(weight_gram AS INT64) AS weight_gram,
    CAST(is_active AS BOOLEAN) AS is_active
FROM {{ source('raw', 'products') }}
