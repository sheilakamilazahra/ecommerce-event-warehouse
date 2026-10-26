
WITH source_data AS (
    SELECT *
    FROM {{ source('ecommerce_source', 'raw_orders') }}
),

cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,
        CAST(order_ts AS TIMESTAMPTZ) AS order_ts,
        CAST(order_ts AS DATE) AS order_date,
        TRIM(product_category) AS product_category,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(unit_price AS DECIMAL(12, 2)) AS unit_price,
        CAST(discount_pct AS DECIMAL(5, 4)) AS discount_pct,
        TRIM(payment_method) AS payment_method,
        LOWER(TRIM(status)) AS status
    FROM source_data
)

SELECT
    *,
    CAST(quantity * unit_price AS DECIMAL(14, 2)) AS gross_amount,
    CAST(
        quantity * unit_price * discount_pct
        AS DECIMAL(14, 2)
    ) AS discount_amount,
    CAST(
        quantity * unit_price * (1 - discount_pct)
        AS DECIMAL(14, 2)
    ) AS net_amount
FROM cleaned
