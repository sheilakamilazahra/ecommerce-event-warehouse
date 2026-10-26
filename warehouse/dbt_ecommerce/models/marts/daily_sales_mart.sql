
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(gross_amount), 2) AS total_gross_sales,
    ROUND(SUM(discount_amount), 2) AS total_discount,
    ROUND(SUM(net_amount), 2) AS total_net_sales,
    COUNT(*) FILTER (WHERE status = 'paid') AS paid_orders,
    COUNT(*) FILTER (WHERE status = 'shipped') AS shipped_orders,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_orders,
    COUNT(*) FILTER (WHERE status = 'refunded') AS refunded_orders
FROM {{ ref('fact_orders') }}
GROUP BY order_date
