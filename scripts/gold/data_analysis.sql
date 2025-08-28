-- ============================================================
-- SUMMARY:
-- This SQL script performs multiple types of sales analysis:
--   1. Yearly Sales Trends
--   2. Monthly Sales Trends
--   3. Cumulative & Moving Average Analysis
--   4. Product Performance Analysis
--   5. Part-to-Whole (Category Sales Contribution)
--   6. Customer Segmentation based on spending and engagement
-- ============================================================


-- ============================================================
-- 1. Yearly Sales Trends
-- Summarizes sales, customers, and quantities at the year level
-- ============================================================
SELECT 
	YEAR(order_date) [Order Year],
	SUM(sales_amount) [Total Sales],
	COUNT(DISTINCT customer_key) [Total Customers],
	SUM(quantity) [Quantity]
FROM gold.fact_sales 
WHERE order_date IS NOT NULL 
GROUP BY YEAR(order_date) 
ORDER BY YEAR(order_date)


-- ============================================================
-- 2. Monthly Sales Trends
-- Breaks down sales by year and month for seasonality analysis
-- ============================================================
SELECT
	YEAR(order_date) [Order Year],
	FORMAT(order_date,'MMMM','en-US') [Order Month],
	SUM(sales_amount) [Total Sales],
	COUNT(DISTINCT customer_key) [Total Customers],
	SUM(quantity) [Quantity]
FROM gold.fact_sales 
WHERE order_date IS NOT NULL 
GROUP BY YEAR(order_date), FORMAT(order_date,'MMMM','en-US') 
ORDER BY YEAR(order_date), FORMAT(order_date,'MMMM','en-US') 


-- ============================================================
-- 3. Cumulative Analysis
-- Tracks running totals and moving averages of yearly sales
-- ============================================================
SELECT 
	 [Order Date]
	,[Total Sales]
	,SUM([Total Sales]) OVER (PARTITION BY [Order Date] ORDER BY [Order Date]) [Running Total Sales] 
	,AVG([Average Price]) OVER (ORDER BY [Order Date]) [Moving Average Price]
FROM (
	SELECT 
		 DATETRUNC(year,order_date) [Order Date]
		,SUM(sales_amount) [Total Sales]
		,AVG(price) [Average Price]
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(year,order_date)
) t


-- ============================================================
-- 4. Product Performance Analysis
-- Evaluates each product's sales against its historical average 
-- and compares year-over-year performance
-- ============================================================
WITH yearly_product_sales AS (
	SELECT 
		YEAR(s.order_date) order_year
		,p.product_name
		,SUM(s.sales_amount) current_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
	GROUP BY YEAR(s.order_date), p.product_name
)
SELECT 
	  order_year
	 ,product_name
	 ,current_sales
	 ,AVG(current_sales) OVER (PARTITION BY product_name) avg_product_sales
	 ,current_sales - AVG(current_sales) OVER (PARTITION BY product_name) [diff on current & avg sales]
	 ,CASE 
			WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
			WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
			ELSE 'On Avg' 
	  END [AVG Changes on Sales]
	  ,LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) [Previous Year Sales]
	  ,current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) [Diff Previous Year Sales]
	  ,CASE 
			WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Sales Increasing'
			WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Sales Decreasing'
			ELSE 'No Changes'
	  END [Changes on Sales]
FROM yearly_product_sales
ORDER BY product_name, order_year


-- ============================================================
-- 5. Part-To-Whole (Proportional) Analysis
-- Calculates the percentage contribution of each category to total sales
-- ============================================================
WITH category_sales AS (
	SELECT 
		 p.category
		,SUM(s.sales_amount) [Total Sales]
	FROM gold.fact_sales s 
	LEFT JOIN gold.dim_products p 
	ON s.product_key = p.product_key
	GROUP BY p.category
)
SELECT
	category
	,[Total Sales]
	,CONCAT(ROUND((CAST([Total Sales] AS FLOAT) / SUM([Total Sales]) OVER ()) * 100,2),'%') [Overall Sales Percentages by Category]  
FROM category_sales
ORDER BY [Total Sales] DESC


-- ============================================================
-- 6. Customer Segmentation
-- Groups customers into VIP, Regular, and New segments 
-- based on lifespan and total spending
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
SELECT 
	 [Customer Segmenting] 
	,COUNT(customer_key) [Total Customers] 
FROM (
	SELECT
		 customer_key
		,CASE 
			WHEN Lifespan >= 12 AND [Total Spending] > 5000 THEN 'VIP'
			WHEN Lifespan >= 12 AND [Total Spending] <= 5000 THEN 'Regular'
			ELSE 'New'
		 END [Customer Segmenting]
	FROM customer_spending
) t
GROUP BY [Customer Segmenting]
ORDER BY [Customer Segmenting] DESC
