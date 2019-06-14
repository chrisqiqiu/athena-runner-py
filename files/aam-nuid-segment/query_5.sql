
select
 nuid
,segment as segment_id
,removed
from temp.aam_segments_rn
where rn=1
