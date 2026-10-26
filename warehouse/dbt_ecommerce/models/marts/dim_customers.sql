
SELECT
    customer_id,
    MIN(order_ts) AS first_order_ts,
    MAX(order_ts) AS last_order_ts,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('stg_orders') }}
GROUP BY customer_id
