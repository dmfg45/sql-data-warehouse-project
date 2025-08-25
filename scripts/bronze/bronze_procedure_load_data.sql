-- ================================================
-- Author:       André Graça
-- Create date:  2025-08-23
-- Description:  Load multiple CSV files into bronze tables with error logging
-- ================================================
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    --==========================
    -- Step 0: Ensure log table exists
    --==========================
    PRINT '======================================';
    PRINT 'Loading Bronze Layer';
    PRINT '======================================';
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'bronze_load_error_logs' AND schema_id = SCHEMA_ID('bronze'))
    BEGIN
        CREATE TABLE bronze.bronze_load_error_logs (
            LogID INT IDENTITY(1,1) PRIMARY KEY,
            TableName NVARCHAR(128),
            FilePath NVARCHAR(260),
            ErrorMessage NVARCHAR(MAX),
            ErrorTime DATETIME DEFAULT GETDATE()
        );
    END
    ELSE
    BEGIN
        PRINT '>> Truncating table bronze.bronze_load_error_logs';
        PRINT '===================================================';
        TRUNCATE TABLE bronze.[bronze_load_error_logs]
    END

    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'bronze_load_success_logs' AND schema_id = SCHEMA_ID('bronze'))
    BEGIN
        CREATE TABLE bronze.bronze_load_success_logs (
            LogID INT IDENTITY(1,1) PRIMARY KEY,
            TableName NVARCHAR(128),
            FilePath NVARCHAR(260),
            RowsInserted INT,
            InsertTime DATETIME DEFAULT GETDATE()
        );
    END
    ELSE
    BEGIN
        PRINT '>> Truncating table bronze.bronze_load_success_logs';;
        PRINT '===================================================';
        TRUNCATE TABLE bronze.[bronze_load_success_logs]
    END
     --==========================
    -- Step 1: Declare variables
    --==========================
    DECLARE @TableName NVARCHAR(128);
    DECLARE @SchemaName NVARCHAR(128) = 'bronze';
    DECLARE @FilePath NVARCHAR(260);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @RowsInserted INT;

    --=============================
    -- Step 2: Table → FilePath mapping
    --=============================
    DECLARE @LoadTables TABLE (
        TableName NVARCHAR(128),
        FilePath NVARCHAR(260)
    );

    INSERT INTO @LoadTables (TableName, FilePath)
    VALUES
    ('crm_cust_info',    'C:\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'),
    ('crm_prd_info',     'C:\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'),
    ('crm_sales_details','C:\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'),
    ('erp_cust_az12',    'C:\sql\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'),
    ('erp_loc_a101',     'C:\sql\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'),
    ('erp_px_cat_g1v2',  'C:\sql\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv');

    --=====================================================
    -- Step 3: Cursor to iterate through tables for loading
    --=====================================================
    
    DECLARE load_cur CURSOR FOR
    SELECT TableName, FilePath
    FROM @LoadTables;

    OPEN load_cur;
    FETCH NEXT FROM load_cur INTO @TableName, @FilePath;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- Check if CSV file exists
            DECLARE @FileExists INT;
            EXEC master.dbo.xp_fileexist @FilePath, @FileExists OUTPUT;

            IF @FileExists = 1
            BEGIN
                -- Truncate existing table data
                
                PRINT '>> Truncating table ' + @SchemaName + '.' + @TableName;

                SET @SQL = 'TRUNCATE TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);
                EXEC sp_executesql @SQL;

                -- Bulk insert from CSV
                PRINT '>> Bulk inserting into table ' + @SchemaName + '.' + @TableName
                      + ' from file: ' + @FilePath;
                PRINT '=======================================================';

                SET @SQL = '
                    BULK INSERT ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + '
                    FROM ''' + @FilePath + '''
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = '','',
                        TABLOCK
                    );';

                EXEC sp_executesql @SQL;

                -- Log successful insert
                SET @SQL = 'SELECT @RowsInserted = COUNT(*) FROM ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);
                EXEC sp_executesql @SQL, N'@RowsInserted INT OUTPUT', @RowsInserted=@RowsInserted OUTPUT;

                INSERT INTO bronze.bronze_load_success_logs (TableName, FilePath, RowsInserted)
                VALUES (@TableName, @FilePath, @RowsInserted);
            END
            ELSE
            BEGIN
                -- Log missing file as a warning
                PRINT 'File not found, skipping table ' + @SchemaName + '.' + @TableName
                      + ' → ' + @FilePath;

                INSERT INTO bronze.bronze_load_error_logs (TableName, FilePath, ErrorMessage)
                VALUES (@TableName, @FilePath, 'File not found');
            END
        END TRY
        BEGIN CATCH
            -- Log any error
            INSERT INTO bronze.bronze_load_error_logs (TableName, FilePath, ErrorMessage)
            VALUES (@TableName, @FilePath, ERROR_MESSAGE());

            PRINT 'Error loading table ' + @SchemaName + '.' + @TableName + ': ' + ERROR_MESSAGE();
        END CATCH

        -- Move to next table
        FETCH NEXT FROM load_cur INTO @TableName, @FilePath;
    END

    CLOSE load_cur;
    DEALLOCATE load_cur;

    PRINT 'Bronze layer CSV loading process completed.';
END
GO
