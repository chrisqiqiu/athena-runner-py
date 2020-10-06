select
  nuid
 ,segment
 ,1 as removed
 from aam.segments
 cross join unnest(split(removed_segments, ',')) as t(segment)
 where segment not in ('','5')
 group by 1,2,3