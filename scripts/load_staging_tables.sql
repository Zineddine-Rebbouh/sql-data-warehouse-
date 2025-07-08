-- File: load_staging_tables.sql
-- Description: Loads data from CSV files into staging tables for the SQL Data Warehouse Project
-- Author: Zineddine Rebbouh
-- Created: 2025-07-08
-- Version: 1.7
-- Purpose: Populates staging tables with raw data from CRM and ERP source systems, with load duration tracking
-- Usage: Execute this script in SQL Server Management Studio or similar SQL client
-- Prerequisites:
--   - Database [sql-datawarehouse-project-db] and staging schema must exist
--   - CSV files must be accessible at specified paths
--   - SQL Server service account must have read/write permissions for file paths
-- Notes:
--   - Error logs are written to specified ERRORFILE paths
--   - Load durations are logged in seconds for each table
-- Change Log:
--   - v1.7: Removed staging.LoadLog table and related logging logic, retaining ERRORFILE logging
--   - v1.6: Fixed BULK INSERT syntax in staging.LoadTable by concatenating FilePath and ErrorFilePath
--   - v1.5: Added staging.LoadLog table, enhanced RAISERROR with context
--   - v1.4: Reorganized with helper procedure staging.LoadTable, fixed duplicate StartTime
--   - v1.3: Fixed BULK INSERT syntax for erp_product_maintenance, corrected file path typo
--   - v1.2: Added load duration tracking
--   - v1.1: Enhanced logging and error handling

SET NOCOUNT ON;
GO

-- Set database context
USE [sql-datawarehouse-project-db];
GO

-- Create helper procedure for loading tables
IF OBJECT_ID('staging.LoadTable') IS NOT NULL
    DROP PROCEDURE staging.LoadTable;
GO

CREATE PROCEDURE staging.LoadTable
    @TableName NVARCHAR(128),
    @FilePath NVARCHAR(512),
    @ErrorFilePath NVARCHAR(512),
    @Step INT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2 = SYSDATETIME();
    DECLARE @EndTime DATETIME2;
    DECLARE @DurationSeconds DECIMAL(10, 3);
    DECLARE @RowCount INT;
    DECLARE @SQL NVARCHAR(MAX);

    BEGIN TRY
        -- Validate table existence
        IF OBJECT_ID('staging.' + @TableName) IS NOT NULL
        BEGIN
            PRINT 'Found table staging.' + @TableName + '. Truncating...';
            
            -- Truncate table
            SET @SQL = N'TRUNCATE TABLE staging.' + QUOTENAME(@TableName);
            EXEC sp_executesql @SQL;
            PRINT 'Truncate successful. Starting BULK INSERT...';

            -- Perform BULK INSERT with concatenated file paths
            SET @SQL = N'
                BULK INSERT staging.' + QUOTENAME(@TableName) + N'
                FROM ''' + REPLACE(@FilePath, '''', '''''') + N'''
                WITH (
                    FORMAT = ''CSV'',
                    FIRSTROW = 2,
                    FIELDTERMINATOR = '','',
                    ROWTERMINATOR = ''\n'',
                    TABLOCK,
                    ERRORFILE = ''' + REPLACE(@ErrorFilePath, '''', '''''') + N''',
                    MAXERRORS = 1000
                );';
            EXEC sp_executesql @SQL;

            SET @RowCount = @@ROWCOUNT;
            SET @EndTime = SYSDATETIME();
            SET @DurationSeconds = CAST(DATEDIFF(MILLISECOND, @StartTime, @EndTime) AS DECIMAL(10, 3)) / 1000;

            PRINT 'Loaded ' + CAST(@RowCount AS VARCHAR(10)) + ' rows into staging.' + @TableName + '.';
            PRINT '⏱️ Duration (' + @TableName + '): ' + CAST(@DurationSeconds AS VARCHAR(10)) + ' seconds.';
        END
        ELSE
        BEGIN
            RAISERROR ('ERROR in Step %d: Table staging.%s does not exist.', 16, 1, @Step, @TableName);
        END
    END TRY
    BEGIN CATCH
        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = CAST(DATEDIFF(MILLISECOND, @StartTime, @EndTime) AS DECIMAL(10, 3)) / 1000;
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR ('ERROR in Step %d loading staging.%s: %s', 16, 1, @Step, @TableName, @ErrorMessage);
    END CATCH
END;
GO

-- Start load process
PRINT '=======================';
PRINT 'BEGINNING STAGING LOAD';
PRINT '=======================';
GO

BEGIN TRY
    BEGIN TRANSACTION;
    PRINT 'Transaction started successfully.';

    DECLARE @Step INT = 1;

    ---------------------------------------------------------------
    -- 1. Load staging.crm_cust_info
    ---------------------------------------------------------------
    PRINT 'Step ' + CAST(@Step AS VARCHAR(2)) + ': Loading staging.crm_cust_info ...';
    EXEC staging.LoadTable 
        @TableName = 'crm_cust_info',
        @FilePath = 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv',
        @ErrorFilePath = 'C:\sql-data-warehouse-project\logs\crm_cust_info_error.log',
        @Step = @Step;
    SET @Step += 1;

    ---------------------------------------------------------------
    -- 2. Load staging.crm_prd_info
    ---------------------------------------------------------------
    PRINT 'Step ' + CAST(@Step AS VARCHAR(2)) + ': Loading staging.crm_prd_info ...';
    EXEC staging.LoadTable 
        @TableName = 'crm_prd_info',
        @FilePath = 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv',
        @ErrorFilePath = 'C:\sql-data-warehouse-project\logs\crm_prd_info_error.log',
        @Step = @Step;
    SET @Step += 1;

    ---------------------------------------------------------------
    -- 3. Load staging.crm_sales_details
    ---------------------------------------------------------------
    PRINT 'Step ' + CAST(@Step AS VARCHAR(2)) + ': Loading staging.crm_sales_details ...';
    EXEC staging.LoadTable 
        @TableName = 'crm_sales_details',
        @FilePath = 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv',
        @ErrorFilePath = 'C:\sql-data-warehouse-project\logs\crm_sales_details_error.log',
        @Step = @Step;
    SET @Step += 1;

    ---------------------------------------------------------------
    -- 4. Load staging.erp_customer_info
    ---------------------------------------------------------------
    PRINT 'Step ' + CAST(@Step AS VARCHAR(2)) + ': Loading staging.erp_customer_info ...';
    EXEC staging.LoadTable 
        @TableName = 'erp_customer_info',
        @FilePath = 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv',
        @ErrorFilePath = 'C:\sql-data-warehouse-project\logs\erp_customer_info_error.log',
        @Step = @Step;
    SET @Step += 1;

    ---------------------------------------------------------------
    -- 5. Load staging.erp_customer_country
    ---------------------------------------------------------------
    PRINT 'Step ' + CAST(@Step AS VARCHAR(2)) + ': Loading staging.erp_customer_country ...';
    EXEC staging.LoadTable 
        @TableName = 'erp_customer_country',
        @FilePath = 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv',
        @ErrorFilePath = 'C:\sql-data-warehouse-project\logs\erp_customer_country_error.log',
        @Step = @Step;
    SET @Step += 1;

    ---------------------------------------------------------------
    -- 6. Load staging.erp_product_maintenance
    ---------------------------------------------------------------
    PRINT 'Step ' + CAST(@Step AS VARCHAR(2)) + ': Loading staging.erp_product_maintenance ...';
    EXEC staging.LoadTable 
        @TableName = 'erp_product_maintenance',
        @FilePath = 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv',
        @ErrorFilePath = 'C:\sql-data-warehouse-project\logs\erp_product_maintenance_error.log',
        @Step = @Step;
    SET @Step += 1;

    ---------------------------------------------------------------
    -- Finalize and verify
    ---------------------------------------------------------------
    COMMIT TRANSACTION;
    PRINT '✅ All staging tables loaded successfully and transaction committed.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();

    PRINT '❌ ERROR in Step ' + CAST(@Step AS VARCHAR(2)) + ': ' + @ErrorMessage;
    RAISERROR ('ERROR in Step %d: %s', @ErrorSeverity, @ErrorState, @Step, @ErrorMessage);
END CATCH;
GO

-- Final table existence check
PRINT '==========================';
PRINT 'VERIFYING STAGING TABLES';
PRINT '==========================';

IF EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'staging'
    AND t.name IN (
        'crm_cust_info', 'crm_prd_info', 'crm_sales_details', 
        'erp_customer_info', 'erp_customer_country', 'erp_product_maintenance'
    )
    GROUP BY ()
    HAVING COUNT(*) = 6
)
BEGIN
    PRINT '✅ Final Check: All 6 staging tables exist.';
END
ELSE
BEGIN
    PRINT '❌ Final Check Failed: One or more staging tables are missing.';
END
GO