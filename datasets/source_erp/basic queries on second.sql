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


------
