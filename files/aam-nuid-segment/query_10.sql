

select
  nuid
 , segment
 , removed
 , '18000000' as upper_limit
 , row_number() over(partition by  segment, nuid order by removed) as rn
from temp.aam_segments
where cast( segment as integer) >= 16000000 and cast( segment as integer) < 18000000
 