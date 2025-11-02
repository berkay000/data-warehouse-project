--silver.crm_cust_info
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



------------------------------------------------------
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