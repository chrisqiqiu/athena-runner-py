
    select
        nuid
, segment as segment_id
, removed
, upper_limit
    from temp.aam_segments_rn_1
    where rn=1
union all
    select
        nuid
, segment as segment_id
, removed
, upper_limit
    from temp.aam_segments_rn_2
    where rn=1
union all
    select
        nuid
, segment as segment_id
, removed
, upper_limit
    from temp.aam_segments_rn_3
    where rn=1
union all
    select
        nuid
, segment as segment_id
, removed
, upper_limit
    from temp.aam_segments_rn_4
    where rn=1
union all
    select
        nuid
, segment as segment_id
, removed
, upper_limit
    from temp.aam_segments_rn_5
    where rn=1
 union all
    select
        nuid
, segment as segment_id
, removed
, upper_limit
    from temp.aam_segments_rn_6
    where rn=1
union all
    select
        nuid
, segment as segment_id
, removed
, upper_limit
    from temp.aam_segments_rn_7
    where rn=1
union all
    select
        nuid
, segment as segment_id
, removed
, upper_limit
    from temp.aam_segments_rn_8
    where rn=1
 