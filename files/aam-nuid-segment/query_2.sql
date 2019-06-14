
select
  nuid
 ,segment
 ,0 as removed
 from aam.segments
 cross join unnest(split(qualified_segments, ',')) as t(segment)
 