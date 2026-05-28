-- Monthly revenue trend across all years
SELECT
    year,
    month,
    ROUND(SUM(revenue), 2) AS total_revenue,
    COUNT(DISTINCT invoice) AS num_orders,
    COUNT(DISTINCT customerid) AS num_customers
FROM online_retail
GROUP BY year, month
ORDER BY year, month;