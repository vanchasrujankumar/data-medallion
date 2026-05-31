WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'orders') }}
),

deduped AS (
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY created_at DESC
               ) AS rn
        FROM source
        WHERE order_id IS NOT NULL
    )
    WHERE rn = 1
),

item_values AS (
    SELECT
        order_id,
        CAST(json_extract_string(t.item, '$.quantity') AS INTEGER) AS quantity,
        CAST(json_extract_string(t.item, '$.unit_price') AS DECIMAL(18,2)) AS unit_price
    FROM deduped, UNNEST(json_transform(items, '["JSON"]')) AS t(item)
),

item_totals AS (
    SELECT
        order_id,
        SUM(quantity * unit_price) AS calculated_total
    FROM item_values
    GROUP BY order_id
)

SELECT
    MD5(d.order_id) AS order_key,
    d.order_id,
    d.user_id,
    CAST(d.total_amount AS DECIMAL(18,2)) AS total_amount,
    CAST(COALESCE(it.calculated_total, 0) AS DECIMAL(18,2)) AS validated_total,
    d.currency,
    CAST(d.created_at AS TIMESTAMP) AS created_at,
    CURRENT_TIMESTAMP AS validated_at,
    CASE
        WHEN it.calculated_total IS NULL THEN 'validation_failed'
        WHEN ABS(d.total_amount - it.calculated_total) < 0.01 THEN d.status
        ELSE 'validation_failed'
    END AS status,
    d.shipping_address
FROM deduped d
LEFT JOIN item_totals it ON d.order_id = it.order_id
WHERE d.rn = 1
