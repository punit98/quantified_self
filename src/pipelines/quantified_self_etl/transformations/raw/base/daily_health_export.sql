CREATE OR REFRESH STREAMING TABLE ${raw_schema}.body_measurements
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT *
FROM STREAM read_files(
    '/Volumes/${catalog}/${raw_schema}/daily_health_export',
    format => 'csv',
    header => 'true',
    filenamepattern => 'DailyHealthExport-HealthMetrics
)
