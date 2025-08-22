--==========================================================
-- Step 1: Declare variables that will be used in the script
--==========================================================

DECLARE @TableName NVARCHAR(128);
DECLARE @SchemaName NVARCHAR(128) = 'bronze';
DECLARE @SQL NVARCHAR(MAX);
--===========================================================
-- Step 2: Create a table variable to store table definitions
--===========================================================
DECLARE @Tables TABLE (
    TableName NVARCHAR(128),
    CreateSQL NVARCHAR(MAX)
);
--==========================================================
-- Step 3: Insert table definitions into @Tables
-- Each entry corresponds to one table we want to (re)create
--==========================================================
INSERT INTO @Tables (TableName, CreateSQL)
VALUES
  -- CRM customer information table
('crm_cust_info', '
(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
)'),
  -- CRM product information table
('crm_prd_info', '
(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
)'),
  -- CRM sales details table
('crm_sales_details', '
(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
)'),
  -- ERP customer information table
('erp_cust_az12', '
(
    cid NVARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
)'),
  -- ERP location information table
('erp_loc_a101', '
(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
)'),
  -- ERP product category table
('erp_px_cat_g1v2', '
(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
)');

--=======================================================
-- Step 4: Declare a cursor to iterate through all tables
--=======================================================
DECLARE cur CURSOR FOR
SELECT TableName, CreateSQL FROM @Tables;


OPEN cur;  -- Open the cursor so we can start fetching
FETCH NEXT FROM cur INTO @TableName, @SQL;  -- Load the first row into variables

--==================================================
-- Step 5: Loop through all tables and recreate them
--==================================================
WHILE @@FETCH_STATUS = 0   -- Continue until no more rows to process
BEGIN
    --===========================================
    -- Step 5a: Check if the table already exists
    --===========================================
    IF EXISTS (
        SELECT 1
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = @TableName
          AND s.name = @SchemaName
    )
    BEGIN
        -- Table already exists, so drop it before recreating
        PRINT 'Dropping existing table ' + @SchemaName + '.' + @TableName;

        DECLARE @DropSQL NVARCHAR(MAX) =
            'DROP TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

        -- Execute the DROP TABLE statement dynamically
        EXEC sp_executesql @DropSQL;
    END

    --=======================================
    -- Step 5b: Create the table from scratch
    --=======================================
    PRINT 'Creating table ' + @SchemaName + '.' + @TableName;

    DECLARE @FullSQL NVARCHAR(MAX) =
        'CREATE TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ' + @SQL;

    -- Execute the CREATE TABLE statement dynamically
    EXEC sp_executesql @FullSQL;

    --====================================================
    -- Step 5c: Fetch the next row (next table definition)
    --====================================================
    FETCH NEXT FROM cur INTO @TableName, @SQL;
END

--=====================================
-- Step 6: Cleanup the cursor resources
--=====================================
CLOSE cur;        -- Close the cursor
DEALLOCATE cur;   -- Free the memory/resources allocated for the cursor
