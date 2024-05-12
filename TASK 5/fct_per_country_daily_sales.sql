WITH fct_per_country_daily_sales AS (
    SELECT
        CASE
            WHEN {{ normalized_phone_number('customer_phone') }} LIKE '62%' THEN 'Indonesia'
            WHEN {{ normalized_phone_number('customer_phone') }} LIKE '91%' THEN 'India'
            ELSE NULL
        END AS country,
        DATE(orders.order_date) AS order_date,
        SUM(order_details.quantity) AS total_quantity,
        SUM(order_details.quantity * order_details.price) AS total_revenue
    FROM
        {{ source('store', 'order_details') }} AS order_details
    LEFT JOIN
        {{ source('store', 'orders') }} AS orders 
        ON order_details.order_id = orders.order_id
    LEFT JOIN
        {{ source('store', 'products') }} AS products 
        N order_details.product_id = products.product_id
    LEFT JOIN
        {{ source('store', 'brands') }} AS brands 
        ON products.brand_id = brands.brand_id
    GROUP BY
        country,
        DATE(orders.order_date)
)
SELECT * FROM fct_per_country_daily_sales
