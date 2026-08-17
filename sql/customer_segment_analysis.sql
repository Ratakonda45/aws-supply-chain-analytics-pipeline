-- Revenue performance by customer segment

SELECT
    customer_segment,
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(line_revenue), 2) AS total_revenue,
    ROUND(
        SUM(line_revenue) /
        NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS avg_order_value
FROM supply_chain_curated_db.order_analytics
GROUP BY customer_segment
ORDER BY total_revenue DESC;
