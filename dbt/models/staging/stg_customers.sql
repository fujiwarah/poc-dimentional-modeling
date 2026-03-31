SELECT
    CAST(customer_id AS INT64) AS customer_id,
    CAST(first_name AS STRING) AS first_name,
    CAST(last_name AS STRING) AS last_name,
    CAST(email AS STRING) AS email,
    CAST(phone AS STRING) AS phone,
    CAST(city AS STRING) AS city,
    CAST(prefecture AS STRING) AS prefecture,
    CAST(postal_code AS STRING) AS postal_code,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(gender AS STRING) AS gender,
    CAST(birth_date AS DATE) AS birth_date,
    CAST(registration_channel AS STRING) AS registration_channel
FROM {{ source('raw', 'customers') }}
