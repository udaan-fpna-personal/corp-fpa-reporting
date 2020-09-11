SET n=0 ;


drop table if exists  corpfpa.custom_changes_to_skus ;
create table if not exists  corpfpa.custom_changes_to_skus as 

select 
distinct dsl.listing_id ,   dsl.brand , cm.vertical ,  rt_mis_category, '1' as include 
from common.stream_listing dsl  left join ds_csv_category_mapping cm on dsl.vertical = cm.vertical 
union all 
select 
distinct  dsl.listing_id , dsl.brand , cm.vertical , 'FMCG' as rt_mis_category, '0' as include 
from common.stream_listing dsl  left join ds_csv_category_mapping cm on dsl.vertical = cm.vertical  where dsl.brand='Saffola'  and lower(rt_mis_category) ='sugar & oil'; 

-----------------------------

drop table if exists  corpfpa.daily_rt_txns_categorised;
create table if not exists  corpfpa.daily_rt_txns_categorised  as 


select
				X.order_date ,
				X.order_id,
				X.order_status ,
				X.seller_org_id , 
				X.buyer_org_id,
				X.listing_id ,
				X.category,
				X.vertical ,
				X.brand , 
				X.Include ,
				X.gmv,
				
				case 
				when include='0' 																		then 'Dont Include'
				when category in ('FMCG','Core Staples','Other Staples','Fresh','Sugar & Oil','Meat')	then 'Food'
				when category in ('Pharma')																then 'Pharma'
																										else 'Mario' 
				end as Type, 
				
				case 
				when category in ('Featurephone','LandlinePhones','Refurbishedmobiles','Smartphone','EN-Mobiles & Accessories','Acc|CE','IT') 	then 'Electronics'
				when category in ('Home & Kitchen','Toys','Luggage & Bags')																		then 'GM'
				when category in ('Clothing','Footwear') 																						then 'Lifestyle'
				when category in ('Electrical') 																								then 'Hardware & Electrical' 
																																				else 'Redundant Aggregate' 
				end as type2
	
FROM

			(
			SELECT 
				rmt.order_date_time as order_date,
				rmt.order_id,
				rmt.order_status, 
				rmt.seller_org_id,
				rmt.buyer_org_id,
				rmt.listing_id,
				cmp.vertical,
				cmp.brand,
				cmp.include ,
				case when cmp.rt_mis_category ='Acc & CE' then 'Acc|CE'   else     cmp.rt_mis_category end as category,
				sum(rmt.total_line_amount) as GMV 
			from 
			common.rt_mis_table rmt  
				left join corpfpa.custom_changes_to_skus cmp on cmp.listing_id=rmt.listing_id
			where 
			rmt.order_date_time = date_sub(from_unixtime(cast((unix_timestamp()+19800)/1 as bigint),'yyyy-MM-dd '),${n} )
			group by 1,2,3,4,5,6,7,8,9,10
			having gmv <200000
			)X ;
--------------------------------------------------
drop table if exists  corpfpa.daily_type_lvl_txn_aggregate;
create table if not exists  corpfpa.daily_type_lvl_txn_aggregate as 
select 
				 type , 
				 count(distinct order_id) 					  as orders, 
				 count(distinct buyer_org_id) 				as buyers , 
				 sum(gmv) 									          as gmv , 
				 sum(gmv)/count(distinct order_id) 		as aov 

from corpfpa.daily_rt_txns_categorised 
group by 1 ; 

-------------------------------------------------
drop table if exists  corpfpa.daily_type2_lvl_txn_aggregate;
create table if not exists  corpfpa.daily_type2_lvl_txn_aggregate as 
select 
				 type2 , 
				 count(distinct order_id) 					as orders, 
				 count(distinct buyer_org_id) 			as buyers , 
				 sum(gmv) 									        as gmv , 
				 sum(gmv)/count(distinct order_id) 	as aov 

from corpfpa.daily_rt_txns_categorised 
group by 1 ; 

----------------------------------------------
drop table if exists  corpfpa.daily_cat_lvl_txn_aggregate;
create table if not exists  corpfpa.daily_cat_lvl_txn_aggregate as 


select 
				category , 
				count(distinct order_id)					as orders, 
				count(distinct buyer_org_id)			as buyers , 
				sum(gmv) 									        as gmv , 
				sum(gmv)/count(distinct order_id)	as aov 
				
from corpfpa.daily_rt_txns_categorised 
group by 1  ;

------------------------------------
drop table if exists  corpfpa.daily_retail_metrics;
create table if not exists  corpfpa.daily_retail_metrics as 


Select                      'Script execution  Time :'                        as Entity ,
from_unixtime(cast((unix_timestamp()+19800)/1 as bigint),'yyyy-MM-dd HH:mm')  as orders ,
                                                                          '1' as buyers ,
                                                                          '1' as gmv , 
                                                                          '1' as aov 
                                                                          
 union all 
select 
														'Grand Total' as Entity  , 
				count(distinct order_id) 					as orders, 
				count(distinct buyer_org_id) 			as buyers , 
				sum(gmv) 									        as gmv , 
				sum(gmv)/count(distinct order_id) as aov 
				
from corpfpa.daily_rt_txns_categorised
union all
select * from corpfpa.daily_cat_lvl_txn_aggregate
union all
select * from corpfpa.daily_type_lvl_txn_aggregate 
union all
select * from corpfpa.daily_type2_lvl_txn_aggregate;