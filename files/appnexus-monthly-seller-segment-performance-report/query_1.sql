-- seller segment performance report summary date_syd=<date> --

with lss as (
	select
	  deal_id
	, deal_name
	, buyer_id
	, buyer_name
	, publisher_id
	, publisher_name
	, auction_id_64
	, segment_id
	, segment_name
	, coalesce(se.mdm_segment_type, '[unknown]') as segment_type
	, coalesce(se.mdm_segment_group, '[unknown]') as segment_group
	, cast(impressions as integer) as impression
	, cast(revenue as decimal(18,6)) as revenue_usd
	, log_date_syd
	, log_hour_syd
	, log_date_utc
	, row_number() over (partition by deal_id, auction_id_64, coalesce(se.mdm_segment_group, '[unknown]') order by segment_id) as rn_sg
	from appnexus.seller_segment_log_summary ls
	left join mdm.appnexus_segment se on ls.segment_id = se.src_segment_id
	where log_date_utc >= date_add('day', -1, cast('<date>' as date) )
	and cast(log_date_syd as date) >= cast('<date>' as date)
), rpt as (
	select   
	  deal_id
	, deal_name
	, buyer_id
	, buyer_name
	, publisher_id
	, publisher_name
	, auction_id_64
	, segment_id
	, segment_name
	, segment_type
	, segment_group
	, impression as segment_count
	, case when rn_sg = 1 then impression else 0 end as impression
	, case when rn_sg = 1 then revenue_usd else 0 end as revenue_usd
	, log_date_syd
	, log_hour_syd
	from lss
)


select   
  log_date_syd
, sum(impression) as impression_double_count
, sum(revenue_usd) as revenue_double_count
from rpt
group by log_date_syd
order by log_date_syd