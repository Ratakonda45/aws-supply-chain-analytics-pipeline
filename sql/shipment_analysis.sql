-- Shipment completeness and delivery analysis

SELECT
    order_status,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT CASE
        WHEN shipment_id IS NULL THEN order_id
    END) AS orders_missing_shipment,
    ROUND(
        100.0 *
        COUNT(DISTINCT CASE
            WHEN shipment_id IS NULL THEN order_id
        END)
        /
        COUNT(DISTINCT order_id),
        2
    ) AS missing_shipment_pct,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days
FROM supply_chain_curated_db.order_analytics
GROUP BY order_status
ORDER BY total_orders DESC;
