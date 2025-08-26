  /*
================================================================================
-- Sales Data Warehouse Views Script
--
-- This script creates three key views in the 'gold' schema:
-- 1. gold.fact_sales      : A fact table capturing sales transactions, including
--                           order, shipping, due dates, amounts, quantities, 
--                           and links to product and customer dimensions.
-- 2. gold.dim_products    : A dimension table containing product attributes such
--                           as name, category, subcategory, cost, product line, 
--                           and start date. Includes a surrogate key for joining.
-- 3. gold.dim_customers   : A dimension table containing customer attributes
--                           such as name, country, marital status, gender, birth 
--                           date, and account creation date. Includes a surrogate key.
--
-- Key Features:
-- - Uses DROP VIEW IF EXISTS to safely remove views before creating them.
-- - Fact table links to dimensions using surrogate keys.
-- - Dimension tables generate surrogate keys with ROW_NUMBER().
-- - Includes LEFT JOINs to enrich data with category and location details.
--
-- This script is intended for SQL Server 2016+ and follows a star schema model
-- for analytical reporting.
================================================================================
*/

-- Drop all views if they exist
DROP VIEW IF EXISTS gold.fact_sales;
DROP VIEW IF EXISTS gold.dim_products;
DROP VIEW IF EXISTS gold.dim_customers;
GO

-- Create Dimension Products
CREATE VIEW gold.dim_products AS
SELECT
      ROW_NUMBER() OVER (ORDER BY pdi.prd_start_dt, pdi.prd_start_dt) AS product_key,
      pdi.prd_id AS product_id,
      pdi.prd_key AS product_number,
      pdi.prd_nm AS product_name,
      pdi.cat_id AS category_id,
      pc.cat AS category,
      pc.subcat AS sub_category,
      pc.maintenance,
      pdi.prd_cost AS cost,
      pdi.prd_line AS product_line,
      pdi.prd_start_dt AS start_date
FROM DataWarehouse.silver.crm_prd_info AS pdi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
  ON pdi.cat_id = pc.id
WHERE prd_end_dt IS NULL;
GO

-- Create Dimension Customers
CREATE VIEW gold.dim_customers AS
SELECT 
     ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
     ci.cst_id AS customer_id,
     ci.cst_key AS customer_number,
     ci.cst_firstname AS first_name,
     ci.cst_lastname AS last_name,
     loc.cntry AS country,
     ci.cst_marital_status AS marital_status,
     CASE 
         WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
         ELSE COALESCE(ca.gen,'Unknown')
     END AS gender,
     ca.bdate AS birth_date,
     ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
  ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS loc
  ON ci.cst_key = loc.cid;
GO

-- Create Fact Sales
CREATE VIEW gold.fact_sales AS
SELECT
       sd.sls_ord_num AS order_number,
       pr.product_key,
       cst.customer_key,
       sd.sls_order_dt AS order_date,
       sd.sls_ship_dt AS shipping_date,
       sd.sls_due_dt AS due_date,
       sd.sls_sales AS sales_amount,
       sd.sls_quantity AS quantity,
       sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
  ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cst
  ON sd.sls_cust_id = cst.customer_id;
GO
