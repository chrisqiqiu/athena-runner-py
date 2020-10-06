

select
  nuid
 , segment
 , removed
 , '15977500' as upper_limit
 , row_number() over(partition by  segment, nuid order by removed) as rn
from temp.aam_segments
where cast( segment as integer) >= 15977300 and cast( segment as integer) < 15977500
 