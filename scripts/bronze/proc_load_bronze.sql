/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
	-- ===================================================================
	-- CONFIGURATION: Base path for dataset files
	-- Change only this line to update all file paths
	-- ===================================================================
	declare @base_path nvarchar(500) = 'C:\Projects\SQL Data Warehouse';
	
	-- File path variables for each table
	declare @file_crm_cust_info nvarchar(500);
	declare @file_crm_prd_info nvarchar(500);
	declare @file_crm_sales_details nvarchar(500);
	declare @file_erp_cust_az12 nvarchar(500);
	declare @file_erp_loc_a101 nvarchar(500);
	declare @file_erp_px_cat_g1v2 nvarchar(500);
	
	-- Build file paths
	set @file_crm_cust_info = @base_path + '\datasets\source_crm\cust_info.csv';
	set @file_crm_prd_info = @base_path + '\datasets\source_crm\prd_info.csv';
	set @file_crm_sales_details = @base_path + '\datasets\source_crm\sales_details.csv';
	set @file_erp_cust_az12 = @base_path + '\datasets\source_erp\CUST_AZ12.csv';
	set @file_erp_loc_a101 = @base_path + '\datasets\source_erp\LOC_A101.csv';
	set @file_erp_px_cat_g1v2 = @base_path + '\datasets\source_erp\PX_CAT_G1V2.csv';
	
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = getdate();
		print('================================================================');
		print('Loading Bronze Layer');
		print('================================================================');

		print('----------------------------------------------------------------');
		print('Loading CRM Tables');
		print('----------------------------------------------------------------');
		---------------------------------------------------------------------------
		---------------------------------------------------------------------------
		set @start_time = getdate();
		print('>> Truncating Table: bronze.crm_cust_info')
		truncate table bronze.crm_cust_info

		print('Inserting Data Into: bronze.crm_cust_info')
		exec('bulk insert bronze.crm_cust_info from ''' + @file_crm_cust_info + ''' with (firstrow=2, fieldterminator='','', tablock)');
		set @end_time = getdate();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar));
		print('-------------------------------');
		-------------------------------------------------------------------------
		---------------------------------------------------------------------------
		set @start_time = getdate();
		print('>> Truncating Table: bronze.crm_prd_info')
		truncate table bronze.crm_prd_info

		print('Inserting Data Into: bronze.crm_prd_info')
		exec('bulk insert bronze.crm_prd_info from ''' + @file_crm_prd_info + ''' with (firstrow=2, fieldterminator='','', tablock)');
		set @end_time = getdate();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar));
		print('-------------------------------');
		-------------------------------------------------------------------------
		---------------------------------------------------------------------------
		set @start_time = getdate();
		print('>> Truncating Table: bronze.crm_sales_details')
		truncate table bronze.crm_sales_details

		print('Inserting Data Into: bronze.crm_sales_details')
		exec('bulk insert bronze.crm_sales_details from ''' + @file_crm_sales_details + ''' with (firstrow=2, fieldterminator='','', tablock)');
		set @end_time = getdate();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar));
		print('-------------------------------');
		--------------------------------------------------------------------------
		---------------------------------------------------------------------------
		print('================================================================');
		print('Loading ERP Tables');
		print('================================================================');

		-------------------------------------------------------------------------
		---------------------------------------------------------------------------
		set @start_time = getdate();
		print('>> Truncating Table: bronze.erp_CUST_AZ12')
		truncate table bronze.erp_CUST_AZ12

		print('Inserting Data Into: bronze.erp_CUST_AZ12')
		exec('bulk insert bronze.erp_CUST_AZ12 from ''' + @file_erp_cust_az12 + ''' with (firstrow=2, fieldterminator='','', tablock)');
		set @end_time = getdate();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar));
		print('-------------------------------');
		-------------------------------------------------------------------------
		---------------------------------------------------------------------------
		set @start_time = getdate();
		print('>> Truncating Table: bronze.erp_LOC_A101')
		truncate table bronze.erp_LOC_A101

		print('Inserting Data Into: bronze.erp_LOC_A101')
		exec('bulk insert bronze.erp_LOC_A101 from ''' + @file_erp_loc_a101 + ''' with (firstrow=2, fieldterminator='','', tablock)');
		set @end_time = getdate();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar));
		print('-------------------------------');
		-------------------------------------------------------------------------
		---------------------------------------------------------------------------
		set @start_time = getdate();
		print('>> Truncating Table: bronze.erp_PX_CAT_G1V2')
		truncate table bronze.erp_PX_CAT_G1V2

		print('Inserting Data Into: bronze.erp_PX_CAT_G1V2')
		exec('bulk insert bronze.erp_PX_CAT_G1V2 from ''' + @file_erp_px_cat_g1v2 + ''' with (firstrow=2, fieldterminator='','', tablock)');
		set @end_time = getdate();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar));
		print('-------------------------------');
		---------------------------------------------------------------------------
		---------------------------------------------------------------------------
		set @batch_end_time = getdate();
		print('================================================================');
		print('Loading Bronze Layes is Completed');
		print('		- Total Load Duration:' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds');
		print('================================================================');
	end try
	begin catch
		print('================================================================');
		print('Error Occured During Loading Bronze Layer');
		print('Error Message' + error_message());
		print('Error Number' + cast(error_number()as nvarchar));
		print('Error State' + cast(error_state()as nvarchar));
		print('================================================================');
	end catch
end