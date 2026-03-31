SELECT
    CAST(category_id AS INT64) AS category_id,
    CAST(name AS STRING) AS category_name,
    CAST(parent_id AS INT64) AS parent_id
FROM {{ source('raw', 'categories') }}
