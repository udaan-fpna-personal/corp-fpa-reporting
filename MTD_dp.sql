use corpfpa; 
set period_start=2020-09-01;
set period_end=2020-09-07;



drop table if exists custom_changes_to_skus ;
create table if not exists custom_changes_to_skus as 

select 
distinct dsl.listing_id ,   dsl.brand , cm.vertical ,  rt_mis_category, '1' as include 
from common.stream_listing dsl  left join default.ds_csv_category_mapping cm on dsl.vertical = cm.vertical 
union all 
select 
distinct  dsl.listing_id , dsl.brand , cm.vertical , 'FMCG' as rt_mis_category, '0' as include 
from common.stream_listing dsl  left join default.ds_csv_category_mapping cm on dsl.vertical = cm.vertical  where dsl.brand='Saffola'  and lower(rt_mis_category) ='sugar & oil'
union all 
select 
distinct  dsl.listing_id , dsl.brand , cm.vertical , 'Electrical' as rt_mis_category, '0' as include 
from common.stream_listing dsl  left join default.ds_csv_category_mapping cm on dsl.vertical = cm.vertical 
where dsl.org_id in 
( 'ORGZ2F605Z1HMQK4FW5E9TDGRDN16', 'ORGE9XKRM3M5TC31FK94J8CW9SNR8' , 'ORG1YC0NB2EHQC68ZNR6FM9E716WV' , 'ORGB8DXD3P3VCDTWFDNW7Z1GWW3H1','ORG7FE5ZKM9SVDRXGTBD61DT844WN' ,'ORG4E57V64T05882ZVSX0PYGSRVB5' , 'ORG6BS0X7S99TCQ8GE9GKRHV08E8L' , 'ORG6XEYZSH5N5DF1FJ78KT7JV7V39' ,'ORG2W3YRFFXGJDKH43B5B84ELLLS9', 'ORGK3JLY50YFMQ2JZ5YBS8LS0SWXB','ORG1ZDLV1EKVPQC3ZJCRWNFLP95L4','ORG1ZDLV1EKVPQC3ZJCRWNFLP95L4','ORGK4YQ8PFK5VQY1ZBGYQX62YBWQ1')
AND 
dsl.vertical in
("Soap Dispensers","CircuitBreakers","DoorFittingsKit","Bathroom Shelve","Toilet Paper Holder","Drain Cover","Towel Holder","Soap Dish and Tumbler Holder","Hooks ","Faucets & Taps","Wall and Sink Mixer","Shower","Health Faucets","Aldrop","Latch","Handles","Tower Bolt","Door Stopper","Door Fittings Kit","Door Closer","Hinge","Door Eye","Mortise Locks","Rim Locks","Pad Locks","Cabinet Locks","Door Knobs/Cyllindrical Locks","Cyllinder Locks","Glass Lock","Ledbulb","Ledpanellight","Ledstring","LEDTubelight","Emergencylight","Ledlantern","TubeHolders","BulbHolder","SwitchesandIndicators","Sockets","SwitchPlates","SwitchSocketCombination","MountingBox","DoorChimesandBells","PlugsandPins","RegulatorandDimmer","ExtensionCords","Circuit Breaker","Fans","TowerBolt","Faucet","ElectricalWire")

; 



-----------------------------

drop table if exists daily_rt_txns_categorised2;
create table if not exists daily_rt_txns_categorised2  as 


select
				X.order_date ,
				X.order_id,
				X.order_status ,
				X.shipment_status,
				X.seller_org_id , 
				X.buyer_org_id,
				X.listing_id ,
				X.category,
				X.vertical ,
				X.brand , 
				X.Include ,
				X.gmv ,
				X.shipped_gmv,
				
				case 
				when include='0' 																		then 'Dont Include:1'
				when category in ('FMCG','Core Staples','Other Staples','Fresh','Sugar & Oil','Meat')	then 'Food'
				when category in ('Pharma')																then 'Pharma'
																										else 'Mario' 
				end as Type, 
				
				case 
				when include='0' 																												then 'Dont Include:2'
				when category in ('Featurephone','LandlinePhones','Refurbishedmobiles','Smartphone','Acc|CE','IT') 	then 'Electronics'
				when category in ('Home & Kitchen','Toys','Luggage & Bags')																		then 'GM'
				when category in ('Clothing','Footwear') 																						then 'Lifestyle'
				when category in ('Electrical') 																								then 'Hardware & Electrical' 
																																				else 'Redundant Aggregate' 
				end as type2
	
FROM

			(
			SELECT 
				from_unixtime(cast((rmt.order_date_time+19800000)/1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as order_date,
				rmt.order_id,
				rmt.order_status,
			  	rmt.shipment_status,
				rmt.seller_org_id,
				rmt.buyer_org_id,
				rmt.listing_id,
				cmp.vertical,
				cmp.brand,
				cmp.include ,
				case when cmp.rt_mis_category ='Acc & CE' then 'Acc|CE'   else     cmp.rt_mis_category end as category,
				sum(rmt.total_line_amount) as GMV ,
			  	sum(CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN rmt.total_line_amount  else null END) as shipped_gmv
			from 
			common.mis_table rmt  
				left join custom_changes_to_skus cmp on cmp.listing_id=rmt.listing_id
			where 
			to_date(mis.order_date_time) between to_date('${period_start}') and to_date('${period_end}')
			and rmt.include is null
			group by 1,2,3,4,5,6,7,8,9,10,11
			having gmv <200000
			)X ;
--------------------------------------------------
drop table if exists daily_type_lvl_txn_aggregate2;
create table if not exists daily_type_lvl_txn_aggregate2 as 

select 
				type ,
				count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) ) 																							as orders,
				count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  buyer_org_id else null end) )																						as buyers  , 
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end) 																											as placed_gmv_excl_buyer_cncl , 
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end)/count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) )	as aov,
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then  gmv else null end)														as placed_gmv_excl_buyer_seller_cncl_exp ,
				sum(shipped_gmv) 																																													as shipped_gmv,
				count(distinct(CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id  else null END) ) 																			as shipped_orders,
				count(distinct(CASE WHEN shipment_status='SHIPMENT_DELIVERED' THEN order_id  else null END) ) 																										as delivered_orders 
from daily_rt_txns_categorised2
group by 1 ; 

--------------------------------------------------
drop table if exists daily_type2_lvl_txn_aggregate2;
create table if not exists daily_type2_lvl_txn_aggregate2 as 
select 
				type2 ,
				count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) ) 																							as orders,
				count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  buyer_org_id else null end) ) 																						as buyers  , 
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end) 																											as placed_gmv_excl_buyer_cncl , 
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end)/count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) )	as aov,
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then  gmv else null end)as placed_gmv_excl_buyer_seller_cncl_exp ,
				sum(shipped_gmv) 																																													as shipped_gmv,
				count(distinct(CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id  else null END) ) 																			as shipped_orders,
				count(distinct(CASE WHEN shipment_status='SHIPMENT_DELIVERED' THEN order_id  else null END) ) 																										as delivered_orders 
from daily_rt_txns_categorised2
group by 1 ; 

-----------------------------------

drop table if exists daily_cat_lvl_txn_aggregate2;
create table if not exists daily_cat_lvl_txn_aggregate2 as 
select 
				category ,
				count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) )																							as orders,
				count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  buyer_org_id else null end) )																						as buyers  , 
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end)																											as placed_gmv_excl_buyer_cncl , 
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end)/count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) )	as aov,
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then  gmv else null end)														as placed_gmv_excl_buyer_seller_cncl_exp ,
				sum(shipped_gmv) as shipped_gmv,
				count(distinct(CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id  else null END) )																			as shipped_orders,
				count(distinct(CASE WHEN shipment_status='SHIPMENT_DELIVERED' THEN order_id  else null END) )																										as delivered_orders from daily_rt_txns_categorised2

group by 1 ; 

------------------------------------
drop table if exists period_retail_metrics;
create table if not exists period_retail_metrics as 
select 																																																				   'Grand Total' , 
				count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) ) 																							as orders,
				count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  buyer_org_id else null end) ) 																						as buyers  , 
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end) 																											as placed_gmv_excl_buyer_cncl , 
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end)/count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) ) as aov,
				sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then  gmv else null end)														as placed_gmv_excl_buyer_seller_cncl_exp ,
				sum(shipped_gmv) 																																													as shipped_gmv,
				count(distinct(CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id  else null END) ) 																			as shipped_orders,
				count(distinct(CASE WHEN shipment_status='SHIPMENT_DELIVERED' THEN order_id  else null END) ) 																										as delivered_orders 
from daily_rt_txns_categorised2

union all
select * from daily_cat_lvl_txn_aggregate2
union all
select * from daily_type_lvl_txn_aggregate2 
union all 
select * from daily_type2_lvl_txn_aggregate2 ;
-------------------------------------
drop table if exists yesterdays_verticalwise_summary ;
create table if not exists yesterdays_verticalwise_summary as 
select  
vertical as vertical ,
brand as brand , 
category as category  , 
count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) ) as orders,
count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  buyer_org_id else null end) ) as buyers  , 
sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end) as placed_gmv_excl_buyer_cncl , 
sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  gmv else null end)/count(distinct(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL') then  order_id else null end) ) as aov,
sum(case when order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then  gmv else null end)as placed_gmv_excl_buyer_seller_cncl_exp ,
sum(shipped_gmv) as shipped_gmv  from 
daily_rt_txns_categorised2
group by 1,2,3




