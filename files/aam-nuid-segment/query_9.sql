

select
  nuid
 , segment
 , removed
 , '16000000' as upper_limit
 , row_number() over(partition by  segment, nuid order by removed) as rn
from temp.aam_segments
where cast( segment as integer) >= 15977510 and cast( segment as integer) < 16000000
 