/*
================================================================================
Procedure: [silver].[usp_reload_silver_tables]

Purpose:
    Reload all "silver" tables from the corresponding "bronze" tables in the Data Warehouse.

    Features:
        - Truncate and reload each table.
        - Logs row counts and duration for each table in silver.data_load_success_logs.
        - Logs any errors in silver.data_load_error_logs per table.
        - Prints progress messages and duration for each table.
        - Prints total duration for the entire procedure.

Tables included:
    - crm_sales_details
    - crm_prd_info
    - erp_loc_a101
    - crm_cust_info
    - erp_cust_az12
    - erp_px_cat_g1v2
================================================================================
*/

CREATE OR ALTER PROCEDURE [silver].[usp_reload_silver_tables]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME2, 
            @end_time   DATETIME2, 
            @rows       INT,
            @proc_name  VARCHAR(100) = 'usp_reload_silver_tables',
            @table_nm   VARCHAR(256),
            @proc_start DATETIME2 = SYSDATETIME();

    --------------------------------------------------------------------
    -- Helper: Create logging tables if not exist
    --------------------------------------------------------------------
    IF OBJECT_ID('silver.data_load_success_logs','U') IS NULL
    BEGIN
        CREATE TABLE silver.data_load_success_logs (
            log_id       INT IDENTITY(1,1) PRIMARY KEY,
            procedure_nm VARCHAR(128) NOT NULL,
            table_nm     VARCHAR(256) NOT NULL,
            row_count    INT NOT NULL,
            start_time   DATETIME2 NOT NULL,
            end_time     DATETIME2 NOT NULL,
            duration_sec INT NOT NULL,
            inserted_at  DATETIME2 NOT NULL CONSTRAINT DF_silver_success_inserted_at DEFAULT SYSDATETIME()
        );
    END

    IF OBJECT_ID('silver.data_load_error_logs','U') IS NULL
    BEGIN
        CREATE TABLE silver.data_load_error_logs (
            log_id       INT IDENTITY(1,1) PRIMARY KEY,
            procedure_nm VARCHAR(128) NOT NULL,
            table_nm     VARCHAR(256) NULL,
            error_msg    NVARCHAR(MAX) NOT NULL,
            error_line   INT NULL,
            start_time   DATETIME2 NOT NULL,
            error_time   DATETIME2 NOT NULL CONSTRAINT DF_silver_error_error_time DEFAULT SYSDATETIME()
        );
    END

    --------------------------------------------------------------------
    -- 1. crm_sales_details
    --------------------------------------------------------------------
    BEGIN TRY
        SET @table_nm = 'silver.crm_sales_details';
        PRINT '>>> Loading ' + @table_nm;

        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT sls_ord_num, sls_prd_key, sls_cust_id,
               CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
               CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
               CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
               CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price) ELSE sls_sales END,
               sls_quantity,
               CASE WHEN sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
                    WHEN sls_price <= 0 THEN ABS(sls_price) ELSE sls_price END
        FROM bronze.crm_sales_details;

        SET @rows = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();

        INSERT INTO silver.data_load_success_logs(procedure_nm, table_nm, row_count, start_time, end_time, duration_sec)
        VALUES(@proc_name, @table_nm, @rows, @start_time, @end_time, DATEDIFF(SECOND,@start_time,@end_time));

        PRINT 'Loaded ' + CAST(@rows AS VARCHAR) + ' rows into ' + @table_nm
            + ' in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        INSERT INTO silver.data_load_error_logs(procedure_nm, table_nm, error_msg, error_line, start_time)
        VALUES(@proc_name, @table_nm, ERROR_MESSAGE(), ERROR_LINE(), ISNULL(@start_time, SYSDATETIME()));
        PRINT '❌ Error loading ' + @table_nm + ': ' + ERROR_MESSAGE();
    END CATCH

    --------------------------------------------------------------------
    -- 2. crm_prd_info
    --------------------------------------------------------------------
    BEGIN TRY
        SET @table_nm = 'silver.crm_prd_info';
        PRINT '>>> Loading ' + @table_nm;

        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt
        )
        SELECT prd_id,
               REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
               SUBSTRING(prd_key,7,LEN(prd_key)),
               TRIM(prd_nm),
               ISNULL(prd_cost,0),
               CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'Unknown' END,
               CAST(prd_start_dt AS DATE),
               CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @rows = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();

        INSERT INTO silver.data_load_success_logs(procedure_nm, table_nm, row_count, start_time, end_time, duration_sec)
        VALUES(@proc_name, @table_nm, @rows, @start_time, @end_time, DATEDIFF(SECOND,@start_time,@end_time));

        PRINT 'Loaded ' + CAST(@rows AS VARCHAR) + ' rows into ' + @table_nm
            + ' in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        INSERT INTO silver.data_load_error_logs(procedure_nm, table_nm, error_msg, error_line, start_time)
        VALUES(@proc_name, @table_nm, ERROR_MESSAGE(), ERROR_LINE(), ISNULL(@start_time, SYSDATETIME()));
        PRINT '❌ Error loading ' + @table_nm + ': ' + ERROR_MESSAGE();
    END CATCH

    --------------------------------------------------------------------
    -- 3. erp_loc_a101
    --------------------------------------------------------------------
    BEGIN TRY
        SET @table_nm = 'silver.erp_loc_a101';
        PRINT '>>> Loading ' + @table_nm;

        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT REPLACE(cid,'-',''),
               CASE WHEN UPPER(TRIM(cntry)) IN ('DE') THEN 'Germany'
                    WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
                    WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'Unknown'
                    ELSE TRIM(cntry) END
        FROM bronze.erp_loc_a101;

        SET @rows = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();

        INSERT INTO silver.data_load_success_logs(procedure_nm, table_nm, row_count, start_time, end_time, duration_sec)
        VALUES(@proc_name, @table_nm, @rows, @start_time, @end_time, DATEDIFF(SECOND,@start_time,@end_time));

        PRINT 'Loaded ' + CAST(@rows AS VARCHAR) + ' rows into ' + @table_nm
            + ' in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        INSERT INTO silver.data_load_error_logs(procedure_nm, table_nm, error_msg, error_line, start_time)
        VALUES(@proc_name, @table_nm, ERROR_MESSAGE(), ERROR_LINE(), ISNULL(@start_time, SYSDATETIME()));
        PRINT '❌ Error loading ' + @table_nm + ': ' + ERROR_MESSAGE();
    END CATCH

    --------------------------------------------------------------------
    -- 4. crm_cust_info
    --------------------------------------------------------------------
    BEGIN TRY
        SET @table_nm = 'silver.crm_cust_info';
        PRINT '>>> Loading ' + @table_nm;

        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        SELECT cst_id, cst_key,
               TRIM(cst_firstname),
               TRIM(cst_lastname),
               CASE UPPER(TRIM(cst_marital_status))
                    WHEN 'S' THEN 'Single'
                    WHEN 'M' THEN 'Married'
                    ELSE 'Unknown' END,
               CASE UPPER(TRIM(cst_gndr))
                    WHEN 'F' THEN 'Female'
                    WHEN 'M' THEN 'Male'
                    ELSE 'Unknown' END,
               cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info
        ) t
        WHERE rn = 1 AND cst_id IS NOT NULL;

        SET @rows = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();

        INSERT INTO silver.data_load_success_logs(procedure_nm, table_nm, row_count, start_time, end_time, duration_sec)
        VALUES(@proc_name, @table_nm, @rows, @start_time, @end_time, DATEDIFF(SECOND,@start_time,@end_time));

        PRINT 'Loaded ' + CAST(@rows AS VARCHAR) + ' rows into ' + @table_nm
            + ' in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        INSERT INTO silver.data_load_error_logs(procedure_nm, table_nm, error_msg, error_line, start_time)
        VALUES(@proc_name, @table_nm, ERROR_MESSAGE(), ERROR_LINE(), ISNULL(@start_time, SYSDATETIME()));
        PRINT '❌ Error loading ' + @table_nm + ': ' + ERROR_MESSAGE();
    END CATCH

    --------------------------------------------------------------------
    -- 5. erp_cust_az12
    --------------------------------------------------------------------
    BEGIN TRY
        SET @table_nm = 'silver.erp_cust_az12';
        PRINT '>>> Loading ' + @table_nm;

        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END,
               CASE WHEN bdate >= GETDATE() THEN NULL
                    WHEN bdate <= '1925-01-01' THEN NULL
                    ELSE bdate END,
               CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                    ELSE 'Unknown' END
        FROM bronze.erp_cust_az12;

        SET @rows = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();

        INSERT INTO silver.data_load_success_logs(procedure_nm, table_nm, row_count, start_time, end_time, duration_sec)
        VALUES(@proc_name, @table_nm, @rows, @start_time, @end_time, DATEDIFF(SECOND,@start_time,@end_time));

        PRINT 'Loaded ' + CAST(@rows AS VARCHAR) + ' rows into ' + @table_nm
            + ' in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        INSERT INTO silver.data_load_error_logs(procedure_nm, table_nm, error_msg, error_line, start_time)
        VALUES(@proc_name, @table_nm, ERROR_MESSAGE(), ERROR_LINE(), ISNULL(@start_time, SYSDATETIME()));
        PRINT '❌ Error loading ' + @table_nm + ': ' + ERROR_MESSAGE();
    END CATCH

    --------------------------------------------------------------------
    -- 6. erp_px_cat_g1v2
    --------------------------------------------------------------------
    BEGIN TRY
        SET @table_nm = 'silver.erp_px_cat_g1v2';
        PRINT '>>> Loading ' + @table_nm;

        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @rows = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();

        INSERT INTO silver.data_load_success_logs(procedure_nm, table_nm, row_count, start_time, end_time, duration_sec)
        VALUES(@proc_name, @table_nm, @rows, @start_time, @end_time, DATEDIFF(SECOND,@start_time,@end_time));

        PRINT 'Loaded ' + CAST(@rows AS VARCHAR) + ' rows into ' + @table_nm
            + ' in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        INSERT INTO silver.data_load_error_logs(procedure_nm, table_nm, error_msg, error_line, start_time)
        VALUES(@proc_name, @table_nm, ERROR_MESSAGE(), ERROR_LINE(), ISNULL(@start_time, SYSDATETIME()));
        PRINT '❌ Error loading ' + @table_nm + ': ' + ERROR_MESSAGE();
    END CATCH

    --------------------------------------------------------------------
    -- Final total duration
    --------------------------------------------------------------------
    DECLARE @proc_end DATETIME2 = SYSDATETIME();
    PRINT '================================================';
    PRINT 'Reload process finished.';
    PRINT 'Total time spent (seconds): ' + CAST(DATEDIFF(SECOND,@proc_start,@proc_end) AS VARCHAR);
    PRINT '================================================';
END
GO
