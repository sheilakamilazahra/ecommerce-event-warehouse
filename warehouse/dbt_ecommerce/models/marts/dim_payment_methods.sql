
SELECT
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('stg_orders') }}
GROUP BY payment_method
