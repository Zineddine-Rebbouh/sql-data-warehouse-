-- File: create_database.sql
-- Description: Initializes the SQL Data Warehouse Project database and its schemas
-- Author: Zineddine Rebbouh
-- Created: 2025-07-08
-- Version: 1.0
-- Purpose: Sets up the database and schema structure for the data warehouse project
-- Usage: Execute this script in SQL Server Management Studio or similar SQL client
-- Notes: Ensure proper permissions are granted to the executing user

SET NOCOUNT ON;
GO

-- 1. Create Database
-- Purpose: Creates the main database for the data warehouse project
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'sql-datawarehouse-project-db')
BEGIN
    BEGIN TRY
        CREATE DATABASE [sql-datawarehouse-project-db];
        PRINT 'Database sql-datawarehouse-project-db created successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating database: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
ELSE
BEGIN
    PRINT 'Database sql-datawarehouse-project-db already exists.';
END
GO

-- 2. Switch to the Created Database
USE [sql-datawarehouse-project-db];
GO

-- 3. Create Schemas

-- Staging Schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'staging')
BEGIN
    BEGIN TRY
        EXEC('CREATE SCHEMA staging AUTHORIZATION dbo;');
        PRINT 'Schema staging created successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating staging schema: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
ELSE
BEGIN
    PRINT 'Schema staging already exists.';
END
GO

-- Data Warehouse Schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw')
BEGIN
    BEGIN TRY
        EXEC('CREATE SCHEMA dw AUTHORIZATION dbo;');
        PRINT 'Schema dw created successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating dw schema: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
ELSE
BEGIN
    PRINT 'Schema dw already exists.';
END
GO

-- Sales Schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw_sales')
BEGIN
    BEGIN TRY
        EXEC('CREATE SCHEMA dw_sales AUTHORIZATION dbo;');
        PRINT 'Schema dw_sales created successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating dw_sales schema: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
ELSE
BEGIN
    PRINT 'Schema dw_sales already exists.';
END
GO

-- Customer Schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw_customer')
BEGIN
    BEGIN TRY
        EXEC('CREATE SCHEMA dw_customer AUTHORIZATION dbo;');
        PRINT 'Schema dw_customer created successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating dw_customer schema: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
ELSE
BEGIN
    PRINT 'Schema dw_customer already exists.';
END
GO

-- Product Schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw_product')
BEGIN
    BEGIN TRY
        EXEC('CREATE SCHEMA dw_product AUTHORIZATION dbo;');
        PRINT 'Schema dw_product created successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating dw_product schema: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
ELSE
BEGIN
    PRINT 'Schema dw_product already exists.';
END
GO
