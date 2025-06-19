/*
----------------------------------------------------------------------
cleaning and transforming
----------------------------------------------------------------------
*/

---------------------------------------------------------------------
-----------------------crm_cust_info--------------------------------
---------------------------------------------------------------------

--Test 1 : check if primary key was repeated or is null

select cst_id , count(*) 
from BRONZE.crm_cust_info
GROUP BY  cst_id
HAVING count(*) > 1

select * from bronze

--SOUTION : we keepp the latest by date of the same id

select *
from (
select * , row_number() over (partition by cst_id order by cst_create_date desc) as flag
from bronze.crm_cust_info
where cst_id is not null 
) where flag =1    -- flag 1 means they are the most latest value of the repetation or the only value 


--------------------------------------------------------------


--- test 2 : the string values have a unwanted spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

-----solution we trim those values

---------------------------------------------------------------------
-------test 3 : change gndr from F to Female & M to Male & null to unknown
-------test 4 : change marital_status from S to Single & M to Married & null to unknown
----------------------------------------------------------------------
------------------Implement soln for Test 1 & 2 & 3 & 4---------------
insert into silver.crm_cust_info(
select 
cst_id,
cst_key ,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,


CASE
when UPPER(cst_marital_status) = 'S' then 'SINGLE'
when UPPER(cst_marital_status) = 'M'  then 'Married'
ELSE 'UNKNOWN'
END
cst_marital_status,

CASE
when UPPER(cst_gndr) = 'F' then 'Female'
when UPPER(cst_gndr) = 'M'  then 'Male'
ELSE 'UNKNOWN'
END
cst_gndr,
cst_create_date
from 
(
select * , row_number() over (partition by cst_id order by cst_create_date desc) as flag
from bronze.crm_cust_info
where cst_id is not null 
) where flag =1    -- flag 1 means they are the most latest value of the repetation or the only value 

)

select * from silver.crm_cust_info

