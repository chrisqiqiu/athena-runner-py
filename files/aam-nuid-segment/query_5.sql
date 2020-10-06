

select
  nuid
 , segment
 , removed
 , '15800000' as upper_limit
 , row_number() over(partition by  segment, nuid order by removed) as rn
from temp.aam_segments
where cast( segment as integer) >= 15000000 and cast( segment as integer) < 15800000
 