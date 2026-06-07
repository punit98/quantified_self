CREATE OR REFRESH STREAMING TABLE ${raw_schema}.WorkoutLog
AS SELECT *
FROM STREAM read_files(
  "/Volumes/${catalog}/${raw_schema}/landing_zone",
  format => "csv",
  header => "true",
  inferSchema => "true"
)