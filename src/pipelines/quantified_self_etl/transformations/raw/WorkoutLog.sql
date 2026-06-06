create or refresh streaming table {raw_schema}.WorkoutLog
as select *

FROM STREAM read_files(
  "/Volumes/{catalog}/{raw_schema}/landing_zone"
  format => "csv",
  header => "true",
  inferSchema => "true"
)