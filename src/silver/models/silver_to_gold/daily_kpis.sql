WITH daily_events AS (
    SELECT
        CAST(event_timestamp AS DATE) AS event_date,
        event_type,
        COUNT(DISTINCT user_id) AS daily_active_users,
        COUNT(*) AS total_events
    FROM {{ ref('clean_events') }}
    GROUP BY 1, 2
),

daily_conversion AS (
    SELECT
        CAST(event_timestamp AS DATE) AS event_date,
        COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS total_page_views,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases
    FROM {{ ref('clean_events') }}
    GROUP BY 1
)

SELECT
    de.event_date,
    de.event_type,
    de.daily_active_users,
    de.total_events,
    ROUND(
        COALESCE(dc.total_purchases, 0) / NULLIF(dc.total_page_views, 0),
        4
    ) AS conversion_rate
FROM daily_events de
LEFT JOIN daily_conversion dc ON de.event_date = dc.event_date
ORDER BY de.event_date, de.event_type
