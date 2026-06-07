CREATE OR REFRESH STREAMING TABLE ${raw_schema}.base__body_measurements
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT *
FROM STREAM read_files(
  "/Volumes/${catalog}/${raw_schema}/landing_zone",
  format => "csv",
  header => "true",
  inferSchema => "true",
  fileNamePattern => "body_measurements.csv"
)
