/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_customers;

GO

CREATE VIEW gold.dim_customers AS

SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	lo.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date

FROM silver.crm_cust_info AS ci

LEFT JOIN silver.erp_cust_az12 AS ca
ON ca.cid = ci.cst_key

LEFT JOIN silver.erp_loc_a101 AS lo
ON lo.cid = ci.cst_key;

GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- ==========================================================================

DROP VIEW IF EXISTS gold.dim_products;

GO

CREATE VIEW gold.dim_products AS

SELECT
	ROW_NUMBER() OVER(ORDER BY prd.prd_start_dt, prd.prd_key) AS product_key,
	prd.prd_id AS product_id,
	prd.prd_key AS product_number,
	prd.prd_nm AS product_name,
	prd.cat_key AS category_id,
	pcat.cat AS category,
	pcat.subcat AS subcategory,
	pcat.maintenance,
	prd.prd_cost AS cost,
	prd.prd_line AS product_line,
	prd.prd_start_dt AS start_date
	

FROM silver.crm_prd_info AS prd

LEFT JOIN silver.erp_px_cat_g1v2 AS pcat
ON pcat.id = prd.cat_key

WHERE prd.prd_end_dt IS NULL; -- Filter out all the historical data

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

DROP VIEW IF EXISTS gold.fact_sales;

GO

CREATE VIEW gold.fact_sales AS

SELECT
	sd.sls_ord_num AS order_number,
	prd.product_key,
	cst.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price

FROM silver.crm_sales_details AS sd

LEFT JOIN gold.dim_products AS prd
ON sd.sls_prd_key = prd.product_number

LEFT JOIN gold.dim_customers AS cst
ON sd.sls_cust_id = cst.customer_id;
