-- Warehouse view: monthly revenue rollup for trend dashboards
CREATE OR REPLACE VIEW v_revenue_trend AS
SELECT
    year(order_date) AS year,
    month(order_date) AS month,
    SUM(total_revenue) AS total_revenue,
    SUM(num_orders) AS num_orders,
    SUM(num_customers) AS num_customers,
    SUM(total_units) AS total_units
FROM daily_revenue
GROUP BY year(order_date), month(order_date)
ORDER BY year, month;

-- Warehouse view: customer RFM segmentation
-- Buckets customers using quartile thresholds for analyst-friendly segments
CREATE OR REPLACE VIEW v_customer_segments AS
WITH scored AS (
    SELECT
        customerid,
        recency_days,
        frequency,
        monetary,
        NTILE(4) OVER (ORDER BY recency_days ASC) AS r_score,
        NTILE(4) OVER (ORDER BY frequency DESC) AS f_score,
        NTILE(4) OVER (ORDER BY monetary DESC) AS m_score
    FROM customer_rfm
)
SELECT
    customerid,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    CASE
        WHEN r_score = 1 AND f_score = 1 AND m_score = 1 THEN 'Champions'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Loyal'
        WHEN r_score = 1 AND f_score >= 3 THEN 'New Customers'
        WHEN r_score = 4 AND f_score = 4 THEN 'Lost'
        WHEN r_score >= 3 AND m_score <= 2 THEN 'At Risk'
        ELSE 'Other'
    END AS segment
FROM scored;

-- Warehouse view: top 50 products by total revenue
CREATE OR REPLACE VIEW v_top_products AS
SELECT
    stockcode,
    description,
    total_revenue,
    total_units,
    num_orders,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM top_products
ORDER BY total_revenue DESC
LIMIT 50;