select
  nuid
 ,segment
 ,1 as removed
 from aam.segments
 cross join unnest(split(removed_segments, ',')) as t(segment)
