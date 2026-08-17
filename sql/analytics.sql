-- ============================================================
-- Supply Chain Analytics Pipeline
-- Amazon Athena Analytics Queries
-- ============================================================

-- 1. Preview curated order analytics data
SELECT *
FROM supply_chain_curated_db.order_analytics
LIMIT 10;


-- 2. Total Revenue
SELECT
    SUM(line_revenue) AS total_revenue
FROM supply_chain_curated_db.order_analytics;


-- 3. Total Orders
SELECT
    COUNT(DISTINCT order_id) AS total_orders
FROM supply_chain_curated_db.order_analytics;


-- 4. Revenue by Product Category
SELECT
    category,
    SUM(line_revenue) AS total_revenue
FROM supply_chain_curated_db.order_analytics
GROUP BY category
ORDER BY total_revenue DESC;


-- 5. Revenue by Warehouse
SELECT
    warehouse_city,
    SUM(line_revenue) AS total_revenue
FROM supply_chain_curated_db.order_analytics
GROUP BY warehouse_city
ORDER BY total_revenue DESC;


-- 6. Orders by Status
SELECT
    order_status,
    COUNT(DISTINCT order_id) AS total_orders
FROM supply_chain_curated_db.order_analytics
GROUP BY order_status
ORDER BY total_orders DESC;


-- 7. Revenue by Customer Segment
SELECT
    customer_segment,
    SUM(line_revenue) AS total_revenue
FROM supply_chain_curated_db.order_analytics
GROUP BY customer_segment
ORDER BY total_revenue DESC;


-- 8. Average Delivery Time
SELECT
    AVG(delivery_days) AS avg_delivery_days
FROM supply_chain_curated_db.order_analytics
WHERE delivery_days IS NOT NULL;
