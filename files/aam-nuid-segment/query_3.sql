  select
    nuid
  , segment
  , removed
  from temp.aam_segments_removed
  where segment not in ('','5')
union all
  select
    nuid
  , segment
  , removed
  from temp.aam_segments_qualified
  where segment not in ('','5')