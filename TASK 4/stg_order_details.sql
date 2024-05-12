WITH base AS (
    SELECT
        orders.order_id,
        DATE(orders.order_date) AS order_date,
        order_details.product_id,
        order_details.quantity,
        order_details.price 
    FROM
        {{ source('store', 'orders') }} AS orders
    LEFT JOIN
        {{ source('store', 'order_details') }} AS order_details
    ON
        orders.order_id = order_details.order_id
),
p_name AS (
    SELECT 
        order_details.product_id,
        products.name AS product_name,
        products.brand_id
    FROM
        {{ source('store', 'order_details') }} AS order_details
    LEFT JOIN
        {{ source('store', 'products') }} AS products
    ON
        order_details.product_id = products.product_id
),
b_name AS(
    SELECT
        products.brand_id,
        brands.name AS brand_name
    FROM
        {{ source('store', 'products') }} AS products
    LEFT JOIN
        {{ source('store', 'brands') }} AS brands
    ON
        products.brand_id = brands.brand_id
)
SELECT
    base.order_date,
    base.quantity,
    base.price,
    b_name.brand_name,
    p_name.product_name
FROM
    base
LEFT JOIN
    p_name
ON
    base.product_id = p_name.product_id
LEFT JOIN
    b_name
ON
    p_name.brand_id = b_name.brand_id
GROUP BY
    base.order_date,
    base.quantity,
    p_name.product_name,
    b_name.brand_name,
    base.price
