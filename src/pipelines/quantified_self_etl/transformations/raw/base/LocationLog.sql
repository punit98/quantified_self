CREATE OR REFRESH STREAMING TABLE ${raw_schema}.LocationLog
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT *
FROM STREAM read_files(
  "/Volumes/${catalog}/${raw_schema}/landing_zone",
  format => "csv",
  header => "true",
  inferSchema => "false",
  fileNamePattern => "LocationLog.csv"
)
