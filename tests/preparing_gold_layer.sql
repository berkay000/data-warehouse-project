----------------------------------------------------------
----------------------------------------------------------
--CUSTOMER

--main query
select
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ca.gen,
	ci.cst_create_date,
	ca.BDATE,
	la.CID
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 ca
	on ci.cst_key = ca.CID
left join silver.erp_LOC_A101 la
	on ci.cst_key = la.CID


--check duplicate
select cst_id, count(*) from (
select
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr --crm is the master for gender info
		else coalesce(ca.gen, 'n/a')
	end as new_gen,
	ci.cst_create_date,
	ca.BDATE,
	la.CID
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 ca
	on ci.cst_key = ca.CID
left join silver.erp_LOC_A101 la
	on ci.cst_key = la.CID
) t
group by cst_id
having count(*) != 1




select distinct
	ci.cst_gndr,
	ca.GEN,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr --crm is the master for gender info
		else coalesce(ca.gen, 'n/a')
	end as new_gen
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 ca
	on ci.cst_key = ca.CID
left join silver.erp_LOC_A101 la
	on ci.cst_key = la.CID
order by cst_gndr, GEN

--final query
select
	row_number() over(order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname last_name,
	la.CID as country,
	ci.cst_marital_status as marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr --crm is the master for gender info
		else coalesce(ca.gen, 'n/a')
	end as gender,
	ca.BDATE as birthdate,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 ca
	on ci.cst_key = ca.CID
left join silver.erp_LOC_A101 la
	on ci.cst_key = la.CID



----------------------------------------------------------
----------------------------------------------------------
--PRODUCT
--main query
select
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.ID,
	pc.CAT,
	pc.SUBCAT,
	pc.MAINTENANCE
from silver.crm_prd_info pn
left join silver.erp_PX_CAT_G1V2 pc
	on pn.cat_id = pc.ID

--filtered query
select
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.ID,
	pc.CAT,
	pc.SUBCAT,
	pc.MAINTENANCE
from silver.crm_prd_info pn
left join silver.erp_PX_CAT_G1V2 pc
	on pn.cat_id = pc.ID
where pn.prd_end_dt is null --filter out all historical data


--check duplicate
select prd_id, count(*) from(
select
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.ID,
	pc.CAT,
	pc.SUBCAT,
	pc.MAINTENANCE
from silver.crm_prd_info pn
left join silver.erp_PX_CAT_G1V2 pc
	on pn.cat_id = pc.ID
where pn.prd_end_dt is null --filter out all historical data
)t 
group by prd_id
having count(*)!=1



--final query
select
	row_number() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number, 
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_PX_CAT_G1V2 pc
	on pn.cat_id = pc.ID
where pn.prd_end_dt is null --filter out all historical data
----------------------------------------------------------
----------------------------------------------------------
--SALES
--final query
select
	sls_ord_num as  order_number,
	pr.product_key,
	cu.customer_key,
	sls_order_dt as order_date,
	sls_ship_dt as shipping_date,
	sls_due_dt as due_date,
	sls_sales as sales_amount,
	sls_quantity as quantity,
	sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
	on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
	on sd.sls_cust_id = cu.customer_id