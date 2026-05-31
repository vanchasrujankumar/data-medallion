WITH customer_events AS (
    SELECT
        user_id,
        MIN(event_timestamp) AS first_seen,
        MAX(event_timestamp) AS last_seen,
        COUNT(*) AS total_events
    FROM {{ ref('clean_events') }}
    GROUP BY user_id
),

top_event_type AS (
    SELECT
        user_id,
        event_type
    FROM (
        SELECT
            user_id,
            event_type,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM {{ ref('clean_events') }}
        GROUP BY user_id, event_type
    )
    WHERE rn = 1
),

customer_orders AS (
    SELECT
        user_id,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN status != 'validation_failed' THEN order_id END) AS valid_orders,
        SUM(CASE WHEN status != 'validation_failed' THEN total_amount ELSE 0 END) AS total_spend
    FROM {{ ref('validate_orders') }}
    GROUP BY user_id
),

all_users AS (
    SELECT DISTINCT user_id FROM {{ ref('clean_events') }}
    UNION
    SELECT DISTINCT user_id FROM {{ ref('validate_orders') }}
)

SELECT
    u.user_id,
    e.first_seen,
    e.last_seen,
    DATEDIFF('day', e.last_seen, CURRENT_TIMESTAMP) AS days_since_last_activity,
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.valid_orders, 0) AS valid_orders,
    COALESCE(o.total_spend, 0) AS total_spend,
    CASE
        WHEN COALESCE(o.valid_orders, 0) > 0
        THEN ROUND(o.total_spend / o.valid_orders, 2)
        ELSE 0
    END AS avg_order_value,
    tet.event_type AS most_common_event_type,
    e.total_events
FROM all_users u
LEFT JOIN customer_events e ON u.user_id = e.user_id
LEFT JOIN top_event_type tet ON u.user_id = tet.user_id
LEFT JOIN customer_orders o ON u.user_id = o.user_id
ORDER BY u.user_id
