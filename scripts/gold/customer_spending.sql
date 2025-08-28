-- ============================================================
-- SUMMARY:
-- This query creates a view (customer_spending) that:
--   1. Aggregates each customer's total spending and activity span
--   2. Identifies their first and last order dates
--   3. Segments customers into categories: VIP, Regular, or New
-- ============================================================

CREATE VIEW gold.customer_spending AS 

-- ============================================================
-- Step 1: Customer-level aggregation
-- Calculate spending, first/last order dates, and lifespan
-- ============================================================
WITH customer_spending  AS (
	SELECT 
		c.customer_key
		,c.first_name + ' ' + c.last_name [Customer Name]
		,SUM(s.sales_amount) [Total Spending]
		,MIN(s.order_date) [First Order Date]
		,MAX(s.order_date) [Last Order Date]
		,DATEDIFF(MONTH, MIN(s.order_date),MAX(s.order_date)) [Lifespan]
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
	GROUP BY c.customer_key, c.first_name + ' ' + c.last_name
)

-- ============================================================
-- Step 2: Final output
-- Select customer details with segmentation rules:
--   - VIP: Lifespan ≥ 12 months and Total Spending > 5000
--   - Regular: Lifespan ≥ 12 months and Total Spending ≤ 5000
--   - New: Lifespan < 12 months
-- ============================================================
SELECT
	 [Customer Name]
	,[Total Spending]
	,Lifespan
	,CASE 
		WHEN Lifespan >= 12 AND [Total Spending] > 5000 THEN 'VIP'
		WHEN Lifespan >= 12 AND [Total Spending] <= 5000 THEN 'Regular'
		ELSE 'New'
	 END [Customer Segmenting]
FROM customer_spending
ORDER BY [Customer Segmenting] DESC
