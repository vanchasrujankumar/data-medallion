WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'events') }}
),

deduped AS (
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY event_id
                   ORDER BY timestamp DESC
               ) AS rn
        FROM source
        WHERE event_id IS NOT NULL
    )
    WHERE rn = 1
),

cleaned AS (
    SELECT
        event_id,
        user_id,
        session_id,
        event_type,
        page,
        referrer,
        CAST(timestamp AS TIMESTAMP) AS event_timestamp,
        TRY_CAST(metadata AS JSON) AS metadata_json,
        CURRENT_TIMESTAMP AS ingested_at
    FROM deduped
    WHERE event_type IN ('page_view', 'click', 'purchase', 'signup', 'error')
)

SELECT
    MD5(event_id) AS event_key,
    *
FROM cleaned
