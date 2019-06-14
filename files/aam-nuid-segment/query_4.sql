select
  nuid
 ,segment
 ,removed
 ,row_number() over(partition by nuid, segment order by removed) as rn
 from temp.aam_segments
 
