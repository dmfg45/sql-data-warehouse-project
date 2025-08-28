-- ============================================================
-- SUMMARY:
-- This query creates a view (products_report) that:
--   1. Combines product details with sales transactions
--   2. Aggregates product-level metrics (orders, sales, customers, lifespan)
--   3. Calculates KPIs such as recency, average order revenue, and monthly revenue
--   4. Segments products into performance categories 
--      (Low, Mid, High, Top Performers)
-- ============================================================

CREATE VIEW gold.products_report AS

-- ============================================================
-- Step 1: Base query
-- Join products with sales to capture transaction-level details
-- ============================================================
WITH base_query AS (
	SELECT
		 p.[product_key]
		,p.[product_name]
		,p.[category_id]
		,p.[category]
		,p.[sub_category]
		,p.[cost]
		,s.order_number
		,s.sales_amount
		,s.quantity
		,s.customer_key
		,s.order_date
	FROM gold.dim_products p
	LEFT JOIN gold.fact_sales s
	ON p.product_key = s.product_key
	WHERE s.price IS NOT NULL
),

-- ============================================================
-- Step 2: Product-level aggregations
-- Summarize orders, sales, quantity, customers, and pricing
-- ============================================================
products_aggregations AS (
	SELECT
		 product_key
		,product_name
		,category
		,sub_category
		,cost
		,order_date
		,COUNT(DISTINCT order_number) [total_orders]
		,SUM(sales_amount) [total_sales]
		,SUM(quantity) [total_quantity]
		,COUNT(DISTINCT customer_key) [total_customers]
		,MAX(order_date) [last_order]
		,MIN(order_date) [first_order]
		,DATEDIFF(month, MIN(order_date), MAX(order_date)) [lifespan]
		,ROUND(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0),1) [average_selling_price]
	FROM base_query
	GROUP BY 
		 product_key
		,product_name
		,category_id
		,category
		,sub_category
		,cost
		,order_date
		,ROUND(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0),1)
)

-- ============================================================
-- Step 3: Final product report
-- Add performance metrics, recency, monthly averages, and classification
-- ============================================================
SELECT
	 product_key
	,product_name
	,category
	,sub_category
	,cost
	,total_orders
	,total_sales
	,total_quantity
	,total_customers
	,lifespan

	-- KPI: Average revenue per order
	,CASE
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	 END [average_order_revenue]

	,average_selling_price

	-- KPI: Recency (months since last order)
	,DATEDIFF(month, last_order, GETDATE()) [recency]

	-- KPI: Average monthly revenue (windowed by month)
	,AVG(total_sales) OVER (PARTITION BY MONTH(order_date) ORDER BY DATETRUNC(month,order_date) DESC) [average_monthly_revenue]

	-- KPI: Alternative monthly revenue calc
	,CASE 
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales / lifespan
	 END [average_monthly_revenue_second]

	-- Product performance segmentation by total sales (4-tier logic)
	,CASE 
		WHEN total_sales <= 5000 THEN 'Low Performers'
		WHEN total_sales > 5000 AND total_sales <= 20000 THEN 'Mid Performers'
		WHEN total_sales > 20000 AND total_sales <= 50000 THEN 'High Performers'
		WHEN total_sales > 50000 THEN 'Top Performers'
	END [performance_sales_products]

FROM products_aggregations
