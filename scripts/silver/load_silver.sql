truncate table silver.crm_cust_info
insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case
	when upper(trim(cst_marital_status)) = 'S' then 'Single'
	when upper(trim(cst_marital_status)) = 'M' then 'Married'
	else 'n/a'
end as cst_marital_status,
case
	when upper(trim(cst_gndr)) = 'F' then 'Female'
	when upper(trim(cst_gndr)) = 'M' then 'Male'
	else 'n/a'
end as cst_gndr,
cst_create_date
from (
select
*,
row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id is not null
) t
where flag_last =1

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
truncate table silver.crm_prd_info
insert into silver.crm_prd_info (
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
select
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
cprd_start_dt,
prd_end_dt
from bronze.crm_prd_info