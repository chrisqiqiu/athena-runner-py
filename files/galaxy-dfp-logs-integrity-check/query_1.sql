-- galaxy dfp logs integrity check day level partition_date=<date> --

with imp_order as (
  
  select 
  orderid  
  , coalesce( ords.name, '' )  as order_name
  ,partition_date
  , date_format(from_unixtime(ni.TimeUsec2/1000000, 'Australia/Sydney'), '%Y-%m-%d') as request_time_sydney
  , date_format(from_unixtime(ni.TimeUsec2/1000000, 'Australia/Sydney'), '%d/%m/%Y') as request_time_sydney
  ,date_format(from_unixtime(ni.EventTimeUsec2/1000000, 'Australia/Sydney'), '%Y-%m-%d') as impression_time_sydney
  ,date_format(from_unixtime(ni.EventTimeUsec2/1000000, 'Australia/Sydney'), '%d/%m/%Y') as impression_time_sydney_f
  from dfp.network_impressions ni 
  left join dfp.service_orders ords on ni.orderid = ords.id
  where partition_date >= cast('<date>' as date)
  and coalesce( ords.name, '' ) like 'G%' 
  and coalesce( ords.name, '' )<>'GM Testing Order'
  
  )


select REGEXP_EXTRACT(order_name,'G(.*?) ',1)  order_no
,order_name
,impression_time_sydney_f as date
, count(*) as row_count
from imp_order
where cast( impression_time_sydney as date) > cast('<date>' as date)
group by 1,2 ,3 
order by 1,3