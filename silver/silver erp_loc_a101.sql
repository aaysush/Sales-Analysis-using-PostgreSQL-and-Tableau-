


----we used '-' in cid whereas we have '' in cid of crm_cust_info
select * from bronze.erp_loc_a101

insert into silver.erp_loc_a101(cid,cntry)
select 
replace (cid,'-','_') cid,
case when trim(cntry) = 'DE' then 'Germany'
     when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) =  '' or cntry is null then 'Unknown'
	 else trim(cntry)
end as cntry
	 
from bronze.erp_loc_a101 -------where replace(cid,'-','') not in 