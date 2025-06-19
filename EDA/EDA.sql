--====================================
--Databasae Exploration
--====================================


------see all schema
SELECT schema_name FROM information_schema.schemata;

------ see all tables
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE';

------see all columns 
SELECT  table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'bronze';

SELECT  table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'silver';

SELECT  table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'gold';



---------see all columns from table 
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'silver'
  AND table_name = 'crm_cust_info';

--====================FINDING UNIQUE AND DITINCT VALUES ==================

------get the countries that our customers come from
select * from gold.dim_customers;
select distinct country from gold.dim_customers;

-----get  the catgories and sub-categories
select distinct category ,subcategory ,product_name from gold.dim_products;

---================Date Exploration (helps in unerstanding span of data)===========
--1)span of order
SELECT
    MIN(order_date) AS first_order,
    MAX(order_date) AS recent_order,
    AGE(MAX(order_date), MIN(order_date)) AS diff
FROM gold.fact_sales;



--=================MEASURE EXPLORATION===================

-- Find the total number of customers that have placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers
FROM gold.fact_sales;

-- Generate a report that shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value
FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity)
FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price)
FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT order_number)
FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(product_name)
FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(customer_key)
FROM gold.dim_customers;




---=================MAGNITUDE PER MEASURE ==========================
----total customers by customer
select country ,count(customer_key) as total_customers
from gold.dim_customers
group by country
order by total_customers desc;

---total customers by gender
select gender ,count(customer_key) as total_customers
from gold.dim_customers
group by gender
order by total_customers desc;

---total products by category
select category ,count(product_key) as total_products
from gold.dim_products
group by category
order by total_products desc;

--==============Ranking==================
-----------highest revenue generating products
select p.product_name , sum(f.sales_amount) as total_revenue
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.product_name
order by total_revenue desc;

-----------lowest revenue generating products
select p.product_name , sum(f.sales_amount) as total_revenue
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.product_name
order by total_revenue asc;
