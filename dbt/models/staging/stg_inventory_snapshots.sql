SELECT
    CAST(snapshot_id AS INT64) AS snapshot_id,
    CAST(product_id AS INT64) AS product_id,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(quantity_on_hand AS INT64) AS quantity_on_hand,
    CAST(quantity_reserved AS INT64) AS quantity_reserved,
    CAST(reorder_point AS INT64) AS reorder_point,
    CAST(lead_time_days AS INT64) AS lead_time_days
FROM {{ source('raw', 'inventory_snapshots') }}
