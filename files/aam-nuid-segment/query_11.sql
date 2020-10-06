

select
  nuid
 , segment
 , removed 
 , '99999999' as upper_limit
 , row_number() over(partition by  segment, nuid order by removed) as rn
from temp.aam_segments
where cast( segment as integer) >= 18000000  
 