```sql date_range
  SELECT 
    DISTINCT(order_purchase_timestamp) AS date
  FROM 
    ecommerce_project.orders
```


```sql total_rev
SELECT
    SUM(payment_value) as total_revenue
FROM 
    ecommerce_project.payments p
JOIN 
    ecommerce_project.orders o
  ON p.order_id = o.order_id
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
```

```sql total_orders
SELECT
    COUNT(DISTINCT(order_id)) as total_orders,
    COUNT(DISTINCT(customer_id)) as total_customers
FROM
    ecommerce_project.orders
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
```

```sql avg_order_value
SELECT
    SUM(payment_value)/COUNT(DISTINCT(p.order_id)) as avg_order_value,
FROM
    ecommerce_project.payments p
JOIN 
    ecommerce_project.orders o
  ON p.order_id = o.order_id
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
```

```sql delivery_time
SELECT 
    ROUND(AVG(delivery_time_days), 1) AS avg_delivery_days
FROM 
    ecommerce_project.orders
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
```


```sql repeat_customers
SELECT COUNT(*) AS repeat_customers
FROM (
    SELECT customer_id
    FROM ecommerce_project.orders
    WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
     GROUP BY customer_id
    HAVING COUNT(order_id) > 1
)
```

```sql daily_rev
SELECT
  CONCAT(
  CAST(EXTRACT(YEAR FROM o.order_purchase_timestamp) AS STRING),
  '-',
  LPAD(CAST(EXTRACT(MONTH FROM o.order_purchase_timestamp) AS STRING), 2, '0'),
  '-',
  LPAD(CAST(EXTRACT(DAY FROM o.order_purchase_timestamp) AS STRING), 2, '0')
) AS day,
  ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM ecommerce_project.orders o
JOIN ecommerce_project.payments p ON o.order_id = p.order_id
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY day
ORDER BY day
```

```sql daily_orders
SELECT
    CONCAT(
      CAST(EXTRACT(YEAR FROM order_purchase_timestamp) AS STRING), '-',
      LPAD(CAST(EXTRACT(MONTH FROM order_purchase_timestamp) AS STRING), 2, '0'), '-',
      LPAD(CAST(EXTRACT(DAY FROM order_purchase_timestamp) AS STRING), 2, '0')
    ) AS day,
    COUNT(order_id) AS order_count
FROM ecommerce_project.orders
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY day
ORDER BY day
```

```sql aov
SELECT
  CONCAT(
    CAST(EXTRACT(YEAR FROM o.order_purchase_timestamp) AS STRING), '-',
    LPAD(CAST(EXTRACT(MONTH FROM o.order_purchase_timestamp) AS STRING), 2, '0'), '-',
    LPAD(CAST(EXTRACT(DAY FROM o.order_purchase_timestamp) AS STRING), 2, '0')
  ) AS day,
  ROUND(SUM(p.payment_value) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
FROM ecommerce_project.orders o
JOIN ecommerce_project.payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY day
ORDER BY day
```

```sql product_category
SELECT 
  REPLACE(UPPER(SUBSTR(pr.product_category_name, 1, 1)) || 
        LOWER(SUBSTR(pr.product_category_name, 2)), '_', ' ') AS category,
  COUNT(oi.product_id) AS total_products_sold
FROM ecommerce_project.order_items oi
JOIN ecommerce_project.products pr ON oi.product_id = pr.product_id
JOIN ecommerce_project.orders o ON oi.order_id = o.order_id
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY category
ORDER BY total_products_sold DESC
LIMIT 10
```

```sql product_revenue
WITH order_item_revenue AS (
  SELECT
    oi.order_id,
    pr.product_category_name AS category,
    oi.price,
    SUM(oi.price) OVER (PARTITION BY oi.order_id) AS order_total_price
  FROM ecommerce_project.order_items oi
  JOIN ecommerce_project.products pr ON oi.product_id = pr.product_id
),

payment_per_order AS (
  SELECT
    order_id,
    SUM(payment_value) AS total_payment
  FROM ecommerce_project.payments
  GROUP BY order_id
)

SELECT 
  REPLACE(
    UPPER(SUBSTR(category, 1, 1)) || LOWER(SUBSTR(category, 2)),
    '_', ' '
  ) AS category,
  ROUND(SUM((oi.price / oi.order_total_price) * p.total_payment), 2) AS total_revenue
FROM order_item_revenue oi
JOIN payment_per_order p ON oi.order_id = p.order_id
WHERE oi.order_id IN (
  SELECT order_id FROM ecommerce_project.orders
  WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
)
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10
```



```sql payment_type
SELECT 
    REPLACE(
    UPPER(SUBSTR(p.payment_type, 1, 1)) || 
    LOWER(SUBSTR(p.payment_type, 2)),'_', ' ') AS payment_type,
    COUNT(*) AS usage_count,
    ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM ecommerce_project.payments p
JOIN ecommerce_project.orders o ON p.order_id = o.order_id
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY p.payment_type
ORDER BY usage_count DESC
```

```sql state_rev
SELECT 
  c.customer_state AS state,
  ROUND(SUM(p.payment_value), 2) AS revenue
FROM ecommerce_project.customers c
JOIN ecommerce_project.orders o ON c.customer_id = o.customer_id
JOIN ecommerce_project.payments p ON o.order_id = p.order_id
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY state
ORDER BY revenue DESC
LIMIT 10
```


```sql city_rev
SELECT 
  UPPER(SUBSTR(c.customer_city, 1, 1)) || LOWER(SUBSTR(c.customer_city, 2)) AS city,
  ROUND(SUM(p.payment_value), 2) AS revenue
FROM ecommerce_project.customers c
JOIN ecommerce_project.orders o ON c.customer_id = o.customer_id
JOIN ecommerce_project.payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp 
  BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY city
ORDER BY revenue DESC
LIMIT 10
```

```sql customers_by_state
SELECT 
  customer_state AS state,
  COUNT(DISTINCT c.customer_id) AS total_customers,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM ecommerce_project.customers c
JOIN ecommerce_project.orders o ON c.customer_id = o.customer_id
JOIN ecommerce_project.payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp 
  BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY state
ORDER BY total_revenue DESC
LIMIT 10
```

```sql customers_by_city
SELECT 
  UPPER(SUBSTR(c.customer_city, 1, 1)) || LOWER(SUBSTR(c.customer_city, 2)) AS city,
  c.customer_state AS state,
  COUNT(DISTINCT c.customer_id) AS total_customers,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM ecommerce_project.customers c
JOIN ecommerce_project.orders o ON c.customer_id = o.customer_id
JOIN ecommerce_project.payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp 
  BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY city, state
ORDER BY total_revenue DESC
LIMIT 10
```

```sql state_summary
SELECT 
  customer_state AS state,
  COUNT(DISTINCT c.customer_id) AS total_customers,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM ecommerce_project.customers c
JOIN ecommerce_project.orders o ON c.customer_id = o.customer_id
JOIN ecommerce_project.payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp 
  BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY state
ORDER BY total_revenue DESC
```

```sql city_summary
SELECT 
  UPPER(SUBSTR(c.customer_city, 1, 1)) || LOWER(SUBSTR(c.customer_city, 2)) AS city,
  c.customer_state AS state,
  COUNT(DISTINCT c.customer_id) AS total_customers,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM ecommerce_project.customers c
JOIN ecommerce_project.orders o ON c.customer_id = o.customer_id
JOIN ecommerce_project.payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp 
  BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY city, state
ORDER BY total_revenue DESC
```


```sql top_sellers_revenue
SELECT 
  seller_id,
  ROUND(SUM(oi.price), 2) AS total_revenue
FROM ecommerce_project.order_items oi
JOIN ecommerce_project.orders o ON oi.order_id = o.order_id
WHERE o.order_purchase_timestamp 
  BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY seller_id
ORDER BY total_revenue DESC
LIMIT 10
```

```sql top_sellers_items
SELECT 
  seller_id,
  COUNT(*) AS total_items_sold
FROM ecommerce_project.order_items oi
JOIN ecommerce_project.orders o ON oi.order_id = o.order_id
WHERE o.order_purchase_timestamp 
  BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY seller_id
ORDER BY total_items_sold DESC
LIMIT 10
```

```sql top_sellers_table
SELECT 
  seller_id,
  ROUND(SUM(oi.price), 2) AS total_revenue,
  COUNT(*) AS total_items_sold
FROM ecommerce_project.order_items oi
JOIN ecommerce_project.orders o ON oi.order_id = o.order_id
WHERE o.order_purchase_timestamp 
  BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
GROUP BY seller_id
ORDER BY total_revenue DESC
```

```sql delivery_time
SELECT 
  CONCAT(
    CAST(EXTRACT(YEAR FROM order_purchase_timestamp) AS STRING), '-',
    LPAD(CAST(EXTRACT(MONTH FROM order_purchase_timestamp) AS STRING), 2, '0'), '-',
    LPAD(CAST(EXTRACT(DAY FROM order_purchase_timestamp) AS STRING), 2, '0')) AS day,
  ROUND(AVG(delivery_time_days), 2) AS avg_delivery_days
FROM ecommerce_project.orders
WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
  AND delivery_time_days IS NOT NULL
GROUP BY day
ORDER BY day
```

```sql product_rev_table
WITH order_item_revenue AS (
  SELECT
    oi.order_id,
    pr.product_category_name AS category,
    oi.price,
    SUM(oi.price) OVER (PARTITION BY oi.order_id) AS order_total_price
  FROM ecommerce_project.order_items oi
  JOIN ecommerce_project.products pr ON oi.product_id = pr.product_id
),

payment_per_order AS (
  SELECT
    order_id,
    SUM(payment_value) AS total_payment
  FROM ecommerce_project.payments
  GROUP BY order_id
)

SELECT 
  REPLACE(
    UPPER(SUBSTR(category, 1, 1)) || LOWER(SUBSTR(category, 2)),
    '_', ' '
  ) AS category,
  COUNT(*) AS total_units_sold,
  ROUND(SUM((oi.price / oi.order_total_price) * p.total_payment), 2) AS total_revenue,
  ROUND(SUM((oi.price / oi.order_total_price) * p.total_payment) / COUNT(*), 2) AS avg_revenue_per_unit
FROM order_item_revenue oi
JOIN payment_per_order p ON oi.order_id = p.order_id
WHERE oi.order_id IN (
  SELECT order_id FROM ecommerce_project.orders
  WHERE order_purchase_timestamp BETWEEN '${inputs.date_filter.start}' AND '${inputs.date_filter.end}'
)
GROUP BY category
ORDER BY total_revenue DESC
```
