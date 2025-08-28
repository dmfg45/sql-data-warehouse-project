-- ============================================================
-- SUMMARY:
-- This query creates a customer report view that:
--   1. Combines sales and customer data
--   2. Aggregates customer-level metrics (orders, sales, products, lifespan)
--   3. Segments customers into age groups and categories (VIP, Regular, New)
--   4. Calculates KPIs like recency, average order value, and monthly spend
-- ============================================================

CREATE VIEW gold.customers_report AS 

-- ============================================================
-- Step 1: Base query - Join fact_sales with customer details
-- Extract order-level information along with customer profile
-- ============================================================
WITH base_query AS (
	SELECT 
		 s.order_number
		,s.product_key
		,s.order_date
		,s.sales_amount
		,s.quantity
		,c.customer_key
		,customer_number
		,CONCAT(c.first_name,' ',c.last_name) [customer_name]
		,DATEDIFF(year,c.birth_date, GETDATE()) [customer_age]
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
),

-- ============================================================
-- Step 2: Customer-level aggregation
-- Summarize total orders, sales, quantity, products, 
-- first/last order dates, and customer lifespan
-- ============================================================
customer_aggregation AS(
SELECT
	 customer_key
	,customer_number
	,customer_name
	,customer_age
	,COUNT (DISTINCT order_number) [total_orders]
	,SUM(sales_amount) [total_sales]
	,SUM(quantity) [total_quantity]
	,COUNT(DISTINCT product_key) [total_products]
	,MAX(order_date) [last_order]
	,MIN(order_date) [first_order]
	,DATEDIFF(month,MIN(order_date), MAX(order_date)) [lifespan]
FROM base_query
GROUP BY customer_key
		,customer_number
		,customer_name
		,customer_age
)

-- ============================================================
-- Step 3: Final customer report
-- Segment customers by age group and engagement level (VIP, Regular, New)
-- Add KPIs such as recency, average order value, and monthly spend
-- ============================================================
SELECT  
	 customer_key
	,customer_number
	,customer_name
	,customer_age
	,CASE
		WHEN customer_age < 20 THEN 'Under 20'
		WHEN customer_age between 20 and 29 THEN '20-29'
		WHEN customer_age between 30 and 39 THEN '30-39'
		WHEN customer_age between 40 and 49 THEN '40-49'
		ELSE '50 And Above'
	 END [age_group]

	,CASE 
		WHEN lifespan >= 12 AND [total_sales] > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND [total_sales] <= 5000 THEN 'Regular'
		ELSE 'New'
	 END [customer_segmenting]

	,first_order
	,last_order
	,DATEDIFF(month,last_order, GETDATE()) [recency]
	,total_orders
	,total_sales
	,total_quantity
	,total_products
	,lifespan
	,total_sales / total_orders [average_order_value]
	,CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales / lifespan 
	 END [average_monthly_spent]
FROM customer_aggregation
