
SELECT
    product_category,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('stg_orders') }}
GROUP BY product_category
