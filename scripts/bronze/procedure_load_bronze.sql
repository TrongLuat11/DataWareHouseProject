/*
======================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
======================================================================================
Scripts Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files/
  IT performs the following actions:
    - Truncates the bronze tables before loading data 
    - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables 

Parameters:
  NONE
This stored procedure does not accept ant parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
======================================================================================
*/
EXEC bronze.load_bronze
-- gọi ra 1 procedure 
-- run procedure => rồi mới run exec 
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
-- TẠO 1 PROCEDURE MỚI CHỨA CÁI BẢNG DỮ LIỆU 
BEGIN
BEGIN TRY 
	DECLARE @start_time DATETIME, @end_time DATETIME,  @batch_start_time DATETIME, @batch_end_time DATETIME;
	-- để tính thời gian từ đầu đến cuối quy trình 

	SET @batch_start_time = GETDATE();
	PRINT'===============================================';
	PRINT'Loading Bronze Layer';
	PRINT'===============================================';

	PRINT'-----------------------------------------------';
	PRINT'Loading CRM Tables';
	PRINT'-----------------------------------------------';

	SET @start_time = GETDATE();
	PRINT'>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	--là một lệnh dùng để xóa toàn bộ dữ liệu trong một bảng mà không xóa cấu trúc của bảng
	-- CẦN LÀM BẢNG TRỐNG TRƯỚC KHI THÊM DỮ LIỆU VÀO 

	PRINT'>> Inserting Data Into: bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\CodeDocuments\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	-- nên để file ở ngoài thay vì trong USER 
	WITH(-- giúp cho lấy dữ liệu từ dòng thứ 2 
		FIRSTROW = 2,
		-- dấu phẩy phân cách giữa cách trường 
		-- FIle Delimiter , ; | # "
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT'>>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
	PRINT'-----------------------------------------------------'

	SET @start_time = GETDATE();
	PRINT'>>Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT'>>Inserting Data Into: bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\CodeDocuments\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT'>>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
	PRINT'-----------------------------------------------------'

	SET @start_time = GETDATE();
	PRINT'>>Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT'>>Inserting Data Into: bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\CodeDocuments\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT'>>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
	PRINT'-----------------------------------------------------'

	PRINT'-----------------------------------------------';
	PRINT'Loading ERP Tables';
	PRINT'-----------------------------------------------';

	SET @start_time = GETDATE();
	PRINT'>>Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT'>>Inserting Data Into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\CodeDocuments\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT'>>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
	PRINT'-----------------------------------------------------'

	SET @start_time = GETDATE();
	PRINT'>>Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT'>>Inserting Data Into: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\CodeDocuments\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT'>>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
	PRINT'-----------------------------------------------------'
	
	SET @start_time = GETDATE();
	PRINT'>>Truncating Table: bronze.erp_px_cat_g1v2'
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT'>>Inserting Data Into: bronze.erp_px_cat_g1v2'
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\CodeDocuments\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
	SET @end_time = GETDATE();
	PRINT'>>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
	PRINT'-----------------------------------------------------'

	SET @batch_end_time = GETDATE();
	PRINT'================================================================'
	PRINT'Loading Bronze Layer is Completed'
	PRINT'    -Total Load Duration:  '+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
	PRINT'================================================================'
	END TRY

	BEGIN CATCH 
		PRINT'========================================'
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT'Error Message' + ERROR_MESSAGE();-- TB LỖI
		PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);-- TB SỐ LỖI
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT'========================================'
	END CATCH 
END
