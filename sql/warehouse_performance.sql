-- Warehouse revenue and fulfillment performance

SELECT
    warehouse_city,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(line_revenue), 2) AS total_revenue,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days
FROM supply_chain_curated_db.order_analytics
GROUP BY warehouse_city
ORDER BY total_revenue DESC;
