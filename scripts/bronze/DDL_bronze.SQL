/*
===============================================================================================================
DDL Scripts: Create Bronze Tables 
===============================================================================================================
Scripts Purpose:
  This scripts creates tables in the 'bronze' schema, dropping existing tables
  if they already exist.
  Run this scripts to re-define the DDL structure of 'bronze' Tables 
===============================================================================================================
*/

--Quy tắc đặt tên tên hệ thống_tên của bảng trong hệ thống
-- Thực hiện câu lệnh IF nếu hệ thống đã có bảng đó rồi 
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL -- gọi tên bảng đó là U, IS NOT NULL: LÀ TỒN TẠI 
	DROP TABLE bronze.crm_cust_info 
CREATE TABLE bronze.crm_cust_info(
--tên của lớp đồng sẽ y như tên trên hệ thống 
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
)
IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_prd_info 
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
)

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL 
	DROP TABLE bronze.crm_sales_details 
CREATE TABLE bronze.crm_sales_details ( 
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_if INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
)

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL 
	DROP TABLE bronze.erp_loc_a101 
CREATE TABLE bronze.erp_loc_a101 ( 
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL 
	DROP TABLE bronze.erp_cust_az12 
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL 
	DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
)
