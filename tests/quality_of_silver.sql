/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--============================================================================================
--============================================================================================
-- silver.crm_cust_info
-- check for nulls or duplicates in primary key
select
cst_id,
count(*) as count
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

-- check for unwanted spaces
select 
*
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)


-- Data Standardization & Consistency
select
distinct cst_gndr
from silver.crm_cust_info
--============================================================================================
--============================================================================================
--silver.crm_prd_info
-- check for nulls or duplicates in primary key
select
prd_id,
count(*) as count
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

-- check for unwanted spaces
select 
*
from silver.crm_prd_info
where prd_nm != trim(prd_nm)

-- check for nulls or negative numbers
select 
*
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- Data Standardization & Consistency
select
distinct prd_line
from silver.crm_prd_info

--check for invalid date orders
select
*
from silver.crm_prd_info
where prd_end_dt < prd_start_dt
--============================================================================================
--============================================================================================
--silver.crm_sales_details
-- check for invalid dates
select 
    nullif(sls_due_dt, 0) as sls_due_dt 
from bronze.crm_sales_details
where sls_due_dt <= 0 
    or LEN(sls_due_dt) != 8 
    or sls_due_dt > 20500101 
    or sls_due_dt < 19000101;

-- check for unwanted spaces
select 
*
from silver.crm_sales_details
where sls_prd_key != trim(sls_prd_key)


--check for invalid date orders
select 
*
from silver.crm_sales_details
where (sls_ship_dt < sls_order_dt) or  (sls_due_dt < sls_ship_dt)


-- Data Consistency: sales, quantity, price
-- sales = quantity * sales
select 
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity  <= 0 or sls_price  <= 0
order by sls_sales, sls_quantity,sls_price
--============================================================================================
--============================================================================================
--silver.erp_CUST_AZ12
-- check for nulls
select
*
from silver.erp_CUST_AZ12
where CID is null
-- check for unwanted spaces
select
*
from silver.erp_CUST_AZ12
where CID != trim(CID)

--Identify out of range dates
select
*
from silver.erp_CUST_AZ12
where bdate >  getdate()

--data standardization and consistency
select
distinct GEN
from silver.erp_CUST_AZ12
--============================================================================================
--============================================================================================
----silver.erp_LOC_A101
-- check for nulls
select
*
from silver.erp_LOC_A101
where CID is null

--data standardization and consistency
select
distinct
CNTRY as old_cntry,
case
	when upper(trim(CNTRY)) = 'DE' then 'Germany'
	when upper(trim(CNTRY)) in ('US', 'USA') then 'United States'
	when trim(CNTRY) = '' or CNTRY is null then 'n/a'
	else trim(CNTRY)
end as CNTRY
from silver.erp_LOC_A101
--============================================================================================
--============================================================================================
--silver.erp_PX_CAT_G1V2
-- check for nulls
select
*
from silver.erp_PX_CAT_G1V2
where ID is null or CAT is null or SUBCAT is null

--check for unwanted spaces
select
*
from silver.erp_PX_CAT_G1V2
where ID != trim(ID) or CAT != trim(CAT) or SUBCAT != trim(SUBCAT) or MAINTENANCE != trim(MAINTENANCE)


--data standardization and consistency
select 
distinct
MAINTENANCE
from silver.erp_PX_CAT_G1V2