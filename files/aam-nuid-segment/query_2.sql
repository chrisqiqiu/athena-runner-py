
select
  nuid
 ,segment
 ,0 as removed
 from aam.segments
 cross join unnest(split(qualified_segments, ',')) as t(segment)
  where segment not in ('','5')
 group by 1,2,3