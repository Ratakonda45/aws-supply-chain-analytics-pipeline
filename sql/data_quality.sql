-- ============================================================
-- Supply Chain Analytics Pipeline
-- Data Quality & Reconciliation Checks
-- ============================================================

-- 1. Count total curated rows
SELECT
    COUNT(*) AS total_rows
FROM supply_chain_curated_db.order_analytics;


-- 2. Count orders with missing shipment information
SELECT
    COUNT(*) AS missing_shipment_rows
FROM supply_chain_curated_db.order_analytics
WHERE shipment_id IS NULL;


-- 3. Missing shipments by order status
SELECT
    order_status,
    COUNT(*) AS missing_shipment_rows
FROM supply_chain_curated_db.order_analytics
WHERE shipment_id IS NULL
GROUP BY order_status
ORDER BY missing_shipment_rows DESC;


-- 4. Distinct affected orders with missing shipment information
SELECT
    order_status,
    COUNT(*) AS affected_rows,
    COUNT(DISTINCT order_id) AS affected_orders
FROM supply_chain_curated_db.order_analytics
WHERE shipment_id IS NULL
GROUP BY order_status
ORDER BY affected_orders DESC;


-- 5. Validate delivered orders against processed shipments
SELECT
    COUNT(DISTINCT o.order_id) AS delivered_orders,
    COUNT(DISTINCT s.order_id) AS delivered_orders_with_shipment,
    COUNT(DISTINCT CASE
        WHEN s.order_id IS NULL THEN o.order_id
    END) AS delivered_orders_without_shipment
FROM supply_chain_processed_d.orders o
LEFT JOIN supply_chain_processed_d.shipments s
    ON o.order_id = s.order_id
WHERE o.order_status = 'Delivered';


-- 6. Compare raw vs processed shipment coverage
SELECT
    (SELECT COUNT(DISTINCT order_id)
     FROM supply_chain_raw_db.shipments)
        AS raw_shipment_orders,

    (SELECT COUNT(DISTINCT order_id)
     FROM supply_chain_processed_d.shipments)
        AS processed_shipment_orders;


-- 7. Validate delivered orders missing shipments in raw data
SELECT
    COUNT(DISTINCT o.order_id) AS delivered_orders_missing_raw_shipment
FROM supply_chain_raw_db.orders o
LEFT JOIN supply_chain_raw_db.shipments s
    ON TRIM(o.order_id) = TRIM(s.order_id)
WHERE TRIM(o.order_status) = 'Delivered'
  AND s.order_id IS NULL;


-- 8. Check duplicate order IDs in curated data
SELECT
    order_id,
    COUNT(*) AS row_count
FROM supply_chain_curated_db.order_analytics
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC;


-- 9. Check null customer IDs
SELECT
    COUNT(*) AS null_customer_ids
FROM supply_chain_curated_db.order_analytics
WHERE customer_id IS NULL;


-- 10. Check invalid or negative revenue
SELECT
    COUNT(*) AS invalid_revenue_rows
FROM supply_chain_curated_db.order_analytics
WHERE line_revenue IS NULL
   OR line_revenue < 0;


-- 11. Check delivery date before shipment date
SELECT
    COUNT(*) AS invalid_delivery_dates
FROM supply_chain_curated_db.order_analytics
WHERE shipment_date IS NOT NULL
  AND delivery_date IS NOT NULL
  AND delivery_date < shipment_date;
