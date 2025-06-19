---Test 1 : checking unwanted spaces
---Test 2 : matching id with other tables


----------it passed all tests----------------

insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
select 
id,
cat,subcat,maintenance

from bronze.erp_px_cat_g1v2