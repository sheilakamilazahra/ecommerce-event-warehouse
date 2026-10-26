
SELECT
    o.order_id,
    o.customer_id,
    o.product_category,
    o.payment_method,
    o.order_ts,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.gross_amount,
    o.discount_amount,
    o.net_amount,
    o.status
FROM {{ ref('stg_orders') }} AS o

INNER JOIN {{ ref('dim_customers') }} AS c
    ON o.customer_id = c.customer_id

INNER JOIN {{ ref('dim_product_categories') }} AS p
    ON o.product_category = p.product_category

INNER JOIN {{ ref('dim_payment_methods') }} AS m
    ON o.payment_method = m.payment_method
