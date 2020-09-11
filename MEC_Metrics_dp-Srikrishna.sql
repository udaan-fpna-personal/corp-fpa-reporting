set period_start='2020-07-01';
set period_end='2020-07-31';
set Month='July-20' ;


drop table if exists corpfpa.all_buyer_lags ;
create table if not exists corpfpa.all_buyer_lags as 

select distinct buyer_org_id, order_month , category ,business_unit ,Cust_cat_phones,Cust_cat_hnk,mario_tag, buyer_cat_month_newrep,buyer_cust_cat_phones_month_newrep,	buyer_bu_month_newrep,	buyer_mario_tag_month_newrep,	 	 buyer_month_newrep		
FROM 
(select buyer_org_id , category , Cust_cat_phones,Cust_cat_hnk, business_unit ,mario_tag,order_month, category_lag ,Cust_cat_phones_lag,BU_lag,mario_lag,overall_lag,
case when concat_ws('+',collect_set(case when buyer_cat_month_rank=1 AND category_lag is NULL then 'NEW' else 'REPEAT' end) over (partition by buyer_org_id , order_month ,category  )) like '%NEW%' then 'New'  else 'Repeat' END 	buyer_cat_month_newrep,		
case when concat_ws('+',collect_set(case when buyer_cust_cat_phones_month_rank=1 AND Cust_cat_phones_lag is NULL then 'NEW' else 'REPEAT' end ) over (partition by buyer_org_id , order_month, cust_cat_phones )) like '%NEW%' then 'New'  else 'Repeat' END 	buyer_cust_cat_phones_month_newrep,	

case when concat_ws('+',collect_set(case when buyer_cust_cat_hnk_month_rank=1 AND Cust_cat_hnk_lag is NULL then 'NEW' else 'REPEAT' end ) over (partition by buyer_org_id , order_month, cust_cat_hnk )) like '%NEW%' then 'New'  else 'Repeat' END 	buyer_cust_cat_hnk,	

case when concat_ws('+',collect_set(case when buyer_BU_month_rank=1 AND bu_lag is NULL then 'NEW' else 'REPEAT' end ) over (partition by buyer_org_id , order_month ,business_unit)) 		like '%NEW%' then 'New'  else 'Repeat' END 	buyer_bu_month_newrep,		
case when concat_ws('+',collect_set(case when buyer_mario_tag_rank=1 AND mario_lag is NULL then 'NEW' else 'REPEAT' end) over (partition by buyer_org_id , order_month,mario_tag )) 		like '%NEW%' then 'New'  else 'Repeat' END 	buyer_mario_tag_month_newrep,	 	
case when concat_ws('+',collect_set(case when buyer_overall_month_rank=1 AND overall_lag is NULL then 'NEW' else 'REPEAT' end ) over (partition by buyer_org_id , order_month  )) 	like '%NEW%' then 'New'  else 'Repeat' END 	buyer_month_newrep		
from 

(select 

buyer_org_id , category ,Cust_cat_phones,Cust_cat_hnk, business_unit ,mario_tag , order_date ,order_month,
datediff(order_date , lag(order_date,1) over (partition by buyer_org_id , category order by order_date asc ))       as category_lag , 
datediff(order_date , lag(order_date,1) over (partition by buyer_org_id , Cust_cat_phones order by order_date asc ))  as Cust_cat_phones_lag , 

datediff(order_date , lag(order_date,1) over (partition by buyer_org_id , Cust_cat_hnk order by order_date asc ))  as Cust_cat_hnk_lag , 

datediff(order_date , lag(order_date,1) over (partition by buyer_org_id , business_unit order by order_date asc ))  as BU_lag ,
datediff(order_date , lag(order_date,1) over (partition by buyer_org_id , mario_tag order by order_date asc ))  as mario_lag ,
datediff(order_date , lag(order_date,1) over (partition by buyer_org_id  order by order_date asc ))                 as overall_lag ,

rank() over (partition by buyer_org_id , order_month  order by order_date asc) as buyer_overall_month_rank , 
rank() over (partition by buyer_org_id , order_month,business_unit  order by order_date asc) as buyer_BU_month_rank , 
rank() over (partition by buyer_org_id , order_month ,Cust_cat_phones  order by order_date asc) as buyer_cust_cat_phones_month_rank ,

rank() over (partition by buyer_org_id , order_month ,Cust_cat_hnk  order by order_date asc) as buyer_cust_cat_hnk_month_rank ,

rank() over (partition by buyer_org_id , order_month ,business_unit , category  order by order_date asc) as buyer_cat_month_rank,
rank() over (partition by buyer_org_id , order_month,mario_tag  order by order_date asc) as buyer_mario_tag_rank 


FROM  
 
(select distinct mis.buyer_org_id , mis.category , ccm.Business_unit ,ccm.Cust_cat_phones,ccm.Cust_cat_hnk, order_date ,mario_tag , order_month
from common.mis_table mis  left join corpfpa.custom_mapping ccm  on mis.category=ccm.category 
where include is null )X
)Y
)Z
;


drop table if exists corpfpa.buyer_cuts_raw ;
create table if not exists corpfpa.buyer_cuts_raw as 

select
mis.buyer_org_id ,
mis.category , 
mrs.business_unit,
mrs.Cust_cat_phones ,
mrs.Cust_cat_hnk ,
mrs.mario_tag,
mrs.buyer_cat_month_newrep , 
mrs.buyer_bu_month_newrep, 
mrs.buyer_month_newrep , 
mrs.buyer_cust_cat_phones_month_newrep,
mrs.buyer_mario_tag_month_newrep,
mis.order_status , 
mis.order_id ,
mis.shipment_status,
mis.payment_mode ,

case when mis.fulfillment_center in('FFC_SELLER','FFC_UDFPJIT','FFC_UDAAN_FLEX') then 'seller' when mis.fulfillment_center in ('FFC_UDWH', 'FFC_UDOTG')
then 'wh' else NULL end as fulfillment_type , 


case when cnc.org_id is not null then 'CNC' else  NULL  END as CNC_tag , 

sum(case when first_shipped is not null then total_line_amount else null end ) as shipped_gmv ,
sum(total_line_amount) as gmv 

from common.mis_table mis
  left join corpfpa.custom_mapping ccm  on mis.category=ccm.category 
  left join corpfpa.all_buyer_lags mrs on mrs.buyer_org_id=mis.buyer_org_id AND ccm.category=mrs.category AND mis.order_month = mrs.order_month 
  left join ds_sql_cnc_org cnc on mis.seller_org_id=cnc.org_id
where include is null 
and order_date between to_date(${period_start}) and to_date(${period_end})
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;


drop table if  exists  corpfpa.past_buyers_data ; 
create table if not exists corpfpa.past_buyers_data as 

select 
concat('category-',category) as entity, count(distinct buyer_org_id) as past_buyers from common.mis_table  
where order_date < to_date(${period_start})
and include is null
group by 1
union all 
select 
concat('BU-',ccm.business_unit) as entity, count(distinct buyer_org_id) as past_buyers from common.mis_table  mis left join corpfpa.custom_mapping ccm on mis.category=ccm.category
where order_date < to_date(${period_start})
and include is null
group by 1
union all 
select 
concat('Custom Category-',ccm.Cust_cat_phones) as entity,count(distinct buyer_org_id) as past_buyers from common.mis_table  mis left join corpfpa.custom_mapping ccm on mis.category=ccm.category
where order_date < to_date(${period_start})
and include is null
and ccm.Cust_cat_phones not in ('Ignore')
group by 1

union all
select 
concat('Custom Category-',ccm.Cust_cat_hnk) as entity,count(distinct buyer_org_id) as past_buyers from common.mis_table  mis left join corpfpa.custom_mapping ccm on mis.category=ccm.category
where order_date < to_date(${period_start})
and include is null
and ccm.Cust_cat_hnk not in ('Ignore')
group by 1
union all


select 
concat('Business Group-',ccm.mario_tag) as entity, count(distinct buyer_org_id) as past_buyers from common.mis_table  mis left join corpfpa.custom_mapping ccm on mis.category=ccm.category
where order_date < to_date(${period_start})
and include is null
group by 1
union all 
select 
'Overall' as Entity , count(distinct buyer_org_id) as past_buyers from common.mis_table                    
where order_date < to_date(${period_start})
and include is null 
group by 1 ;



drop table if exists corpfpa.buyer_cuts_final ;
create table if not exists corpfpa.buyer_cuts_final as 

select X.* , Y.past_buyers FROM 
(

select 
case when category is null then 'category-null' else concat('category-',category) end  as entity ,
count(distinct order_id ) as placed_orders  , 
count(distinct CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id END) as shipped_orders ,
count(distinct case when shipment_status='SHIPMENT_DELIVERED' then order_id END) as delivered_orders , 
sum(case when shipment_status in ('SHIPMENT_RTO_ABSORBED','SHIPMENT_RTO' , 'SHIPMENT_RTO_DELIVERED' , 'SHIPMENT_RTO_TO_SELLER') then gmv else null end ) as rto_gmv ,
sum(case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') then  shipped_gmv else null end) as credit_gmv,

sum(gmv) as placed_gmv,
sum(shipped_gmv) as shipped_gmv,
sum(case when shipment_status='SHIPMENT_DELIVERED' and order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then gmv else null end ) as delivered_gmv ,

sum(case when   cnc_tag is not null then shipped_gmv else null end ) as cnc_gmv,
sum(case when  fulfillment_type='seller' and cnc_tag is null   then shipped_gmv else null end ) as seller_fulfilled_gmv , 
sum(case when  fulfillment_type='wh' and cnc_tag is null   then shipped_gmv else null end ) as warehouse_fulfilled_gmv ,


count(distinct (case when buyer_cat_month_newrep='New' then buyer_org_id else null end)) as New_buyers , 
count(distinct (case when buyer_cat_month_newrep='Repeat' then buyer_org_id else null end)) as Repeat_buyers ,
count(distinct (case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then buyer_org_id else null end)) as Credit_buyers,

count(distinct (case when   cnc_tag is not null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then order_id else null end )) as cnc_orders,
count(distinct (case when  fulfillment_type='seller' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'   then order_id else null end )) as seller_fulfilled_orders , 
count(distinct (case when  fulfillment_type='wh' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'  then order_id else null end )) as warehouse_fulfilled_orders 


FROM 
corpfpa.buyer_cuts_raw x 

group by 1

union all

select case when business_unit is null then 'BU-Null' else concat('BU-',Business_unit) end as entity ,
count(distinct order_id ) as placed_orders  , 
count(distinct CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id END) as shipped_orders ,
count(distinct case when shipment_status='SHIPMENT_DELIVERED' then order_id END) as delivered_orders , 
sum(case when shipment_status in ('SHIPMENT_RTO_ABSORBED','SHIPMENT_RTO' , 'SHIPMENT_RTO_DELIVERED' , 'SHIPMENT_RTO_TO_SELLER') then gmv else null end ) as rto_gmv ,
sum(case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') then  shipped_gmv else null end) as credit_gmv,

sum(gmv) as placed_gmv,
sum(shipped_gmv) as shipped_gmv,
sum(case when shipment_status='SHIPMENT_DELIVERED' and order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then gmv else null end ) as delivered_gmv ,
sum(case when  cnc_tag is not null then shipped_gmv else null end ) as cnc_gmv,
sum(case when fulfillment_type='seller' and cnc_tag is null  then shipped_gmv else null end ) as seller_fulfilled_gmv , 
sum(case when fulfillment_type='wh' and cnc_tag is null  then shipped_gmv else null end ) as warehouse_fulfilled_gmv ,


count(distinct (case when buyer_bu_month_newrep='New' then buyer_org_id else null end)) as New_buyers , 
count(distinct (case when buyer_bu_month_newrep='Repeat' then buyer_org_id else null end)) as Repeat_buyers ,
count(distinct (case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then buyer_org_id else null end)) as Credit_buyers,
count(distinct (case when   cnc_tag is not null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then order_id else null end )) as cnc_orders,
count(distinct (case when  fulfillment_type='seller' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'   then order_id else null end )) as seller_fulfilled_orders , 
count(distinct (case when  fulfillment_type='wh' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'  then order_id else null end )) as warehouse_fulfilled_orders  


FROM 
corpfpa.buyer_cuts_raw 

group by 1
union all 

select case when Cust_cat_phones is null then 'Custom_category_phones-Null' else concat('Custom Category-',Cust_cat_phones) end as entity ,
count(distinct order_id ) as placed_orders  , 
count(distinct CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id END) as shipped_orders ,
count(distinct case when shipment_status='SHIPMENT_DELIVERED' then order_id END) as delivered_orders , 
sum(case when shipment_status in ('SHIPMENT_RTO_ABSORBED','SHIPMENT_RTO' , 'SHIPMENT_RTO_DELIVERED' , 'SHIPMENT_RTO_TO_SELLER') then gmv else null end ) as rto_gmv ,
sum(case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') then  shipped_gmv else null end) as credit_gmv,

sum(gmv) as placed_gmv,
sum(shipped_gmv) as shipped_gmv,
sum(case when shipment_status='SHIPMENT_DELIVERED' and order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then gmv else null end ) as delivered_gmv ,
sum(case when     cnc_tag is not null then shipped_gmv else null end ) as cnc_gmv,
sum(case when    fulfillment_type='seller' and cnc_tag is null  then shipped_gmv else null end ) as seller_fulfilled_gmv , 
sum(case when    fulfillment_type='wh' and cnc_tag is null  then shipped_gmv else null end ) as warehouse_fulfilled_gmv ,


count(distinct (case when buyer_cust_cat_phones_month_newrep='New' then buyer_org_id else null end)) as New_buyers , 
count(distinct (case when buyer_cust_cat_phones_month_newrep='Repeat' then buyer_org_id else null end)) as Repeat_buyers ,
count(distinct (case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then buyer_org_id else null end)) as Credit_buyers,
count(distinct (case when   cnc_tag is not null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then order_id else null end )) as cnc_orders,
count(distinct (case when  fulfillment_type='seller' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'   then order_id else null end )) as seller_fulfilled_orders , 
count(distinct (case when  fulfillment_type='wh' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'  then order_id else null end )) as warehouse_fulfilled_orders 



FROM 
corpfpa.buyer_cuts_raw 

group by 1



union all 

select case when Cust_cat_hnk is null then 'Custom_category_HNK-Null' else concat('Custom Category-',Cust_cat_hnk) end as entity ,
count(distinct order_id ) as placed_orders  , 
count(distinct CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id END) as shipped_orders ,
count(distinct case when shipment_status='SHIPMENT_DELIVERED' then order_id END) as delivered_orders , 
sum(case when shipment_status in ('SHIPMENT_RTO_ABSORBED','SHIPMENT_RTO' , 'SHIPMENT_RTO_DELIVERED' , 'SHIPMENT_RTO_TO_SELLER') then gmv else null end ) as rto_gmv ,
sum(case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') then  shipped_gmv else null end) as credit_gmv,

sum(gmv) as placed_gmv,
sum(shipped_gmv) as shipped_gmv,
sum(case when shipment_status='SHIPMENT_DELIVERED' and order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then gmv else null end ) as delivered_gmv ,
sum(case when     cnc_tag is not null then shipped_gmv else null end ) as cnc_gmv,
sum(case when    fulfillment_type='seller' and cnc_tag is null  then shipped_gmv else null end ) as seller_fulfilled_gmv , 
sum(case when    fulfillment_type='wh' and cnc_tag is null  then shipped_gmv else null end ) as warehouse_fulfilled_gmv ,


count(distinct (case when buyer_cust_cat_phones_month_newrep='New' then buyer_org_id else null end)) as New_buyers , 
count(distinct (case when buyer_cust_cat_phones_month_newrep='Repeat' then buyer_org_id else null end)) as Repeat_buyers ,
count(distinct (case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then buyer_org_id else null end)) as Credit_buyers,
count(distinct (case when   cnc_tag is not null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then order_id else null end )) as cnc_orders,
count(distinct (case when  fulfillment_type='seller' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'   then order_id else null end )) as seller_fulfilled_orders , 
count(distinct (case when  fulfillment_type='wh' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'  then order_id else null end )) as warehouse_fulfilled_orders 



FROM 
corpfpa.buyer_cuts_raw 

group by 1


union all 

select case when mario_tag is null then 'Business Group-NULL' else concat('Business Group-',mario_tag) end as entity ,
count(distinct order_id ) as placed_orders  , 
count(distinct CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id END) as shipped_orders ,
count(distinct case when shipment_status='SHIPMENT_DELIVERED' then order_id END) as delivered_orders , 
sum(case when shipment_status in ('SHIPMENT_RTO_ABSORBED','SHIPMENT_RTO' , 'SHIPMENT_RTO_DELIVERED' , 'SHIPMENT_RTO_TO_SELLER') then gmv else null end ) as rto_gmv ,
sum(case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') then  shipped_gmv else null end) as credit_gmv,

sum(gmv) as placed_gmv,
sum(shipped_gmv) as shipped_gmv,
sum(case when shipment_status='SHIPMENT_DELIVERED' and order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then gmv else null end ) as delivered_gmv ,
sum(case when     cnc_tag is not null then shipped_gmv else null end ) as cnc_gmv,
sum(case when    fulfillment_type='seller' and cnc_tag is null   then shipped_gmv else null end ) as seller_fulfilled_gmv , 
sum(case when    fulfillment_type='wh' and cnc_tag is null  then shipped_gmv else null end ) as warehouse_fulfilled_gmv ,


count(distinct (case when buyer_mario_tag_month_newrep='New' then buyer_org_id else null end)) as New_buyers , 
count(distinct (case when buyer_mario_tag_month_newrep='Repeat' then buyer_org_id else null end)) as Repeat_buyers ,
count(distinct (case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then buyer_org_id else null end)) as Credit_buyers,
count(distinct (case when   cnc_tag is not null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then order_id else null end )) as cnc_orders,
count(distinct (case when  fulfillment_type='seller' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'   then order_id else null end )) as seller_fulfilled_orders , 
count(distinct (case when  fulfillment_type='wh' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'  then order_id else null end )) as warehouse_fulfilled_orders  



FROM 
corpfpa.buyer_cuts_raw 

group by 1


union all 

select 'Overall' as entity ,
count(distinct order_id ) as placed_orders  , 
count(distinct CASE WHEN shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' THEN order_id END) as shipped_orders ,
count(distinct case when shipment_status='SHIPMENT_DELIVERED' then order_id END) as delivered_orders , 
sum(case when shipment_status in ('SHIPMENT_RTO_ABSORBED','SHIPMENT_RTO' , 'SHIPMENT_RTO_DELIVERED' , 'SHIPMENT_RTO_TO_SELLER') then gmv else null end ) as rto_gmv ,
sum(case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') then  shipped_gmv else null end) as credit_gmv,

sum(gmv) as placed_gmv,
sum(shipped_gmv) as shipped_gmv,
sum(case when shipment_status='SHIPMENT_DELIVERED' and order_status NOT in ('SELLER_ORDER_BUYER_CANCEL','SELLER_ORDER_SELLER_CANCEL','SELLER_ORDER_EXPIRED') then gmv else null end ) as delivered_gmv ,
sum(case when    cnc_tag is not null then shipped_gmv else null end ) as cnc_gmv,
sum(case when   fulfillment_type='seller' and cnc_tag is null  then shipped_gmv else null end ) as seller_fulfilled_gmv , 
sum(case when   fulfillment_type='wh' and cnc_tag is null  then shipped_gmv else null end ) as warehouse_fulfilled_gmv ,


count(distinct (case when buyer_month_newrep='New' then buyer_org_id else null end)) as New_buyers , 
count(distinct (case when buyer_month_newrep='Repeat' then buyer_org_id else null end)) as Repeat_buyers ,
count(distinct (case when  payment_mode  IN ('CREDIT_PAY_LATER', 'CREDIT','PAY_LATER') AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then buyer_org_id else null end)) as Credit_buyers,


count(distinct (case when   cnc_tag is not null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS' then order_id else null end )) as cnc_orders,
count(distinct (case when  fulfillment_type='seller' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'   then order_id else null end )) as seller_fulfilled_orders , 
count(distinct (case when  fulfillment_type='wh' and cnc_tag is null AND shipment_status is NOT null AND shipment_status!='SHIPMENT_RTS'  then order_id else null end )) as warehouse_fulfilled_orders 


FROM 
corpfpa.buyer_cuts_raw 

group by 1
)X left join  corpfpa.past_buyers_data Y on X.entity=Y.entity ;

drop table if  exists corpfpa.mec_dataset ;
create table if not exists corpfpa.mec_dataset as 
select 
  'Placed GMV' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,Placed_gmv  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Shipped GMV' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,shipped_gmv  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Delivered GMV' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,Delivered_gmv  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Cash and Carry - GMV' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,cnc_gmv  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Marketplace fullfilled from Udaan WH - GMV' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,warehouse_fulfilled_gmv  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Marketplace fullfilled from Seller - GMV' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,seller_fulfilled_gmv  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'New Buyers' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,New_buyers  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Repeat Buyers' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,Repeat_buyers  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Total Buyers' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,New_buyers+Repeat_buyers  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Placed Orders' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,placed_orders  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Shipped Orders' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,shipped_orders  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Delivered Orders' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,delivered_orders  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Cash and Carry' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,cnc_orders as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Marketplace fullfilled from Udaan WH' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,warehouse_fulfilled_orders  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Marketplace fullfilled from Seller' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,seller_fulfilled_orders as value 
from corpfpa.buyer_cuts_final
union all
select 
  'RTO %' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,rto_gmv  as value 
from corpfpa.buyer_cuts_final
union all
select 
  'Credit GMV %' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,credit_gmv  as value 
from corpfpa.buyer_cuts_final
union all
select 
  '# of Buyers on Credit' Particular , ${Month} Month  ,entity  as category , '' as rprt_category ,'' as remarks ,credit_buyers  as value 
from corpfpa.buyer_cuts_final