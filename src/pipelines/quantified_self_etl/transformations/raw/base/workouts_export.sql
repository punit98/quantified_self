CREATE OR REFRESH STREAMING TABLE ${raw_schema}.workouts_export
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT *
FROM STREAM read_files(
    '/Volumes/${catalog}/${raw_schema}/workouts_export',
    format => 'csv',
    header => 'true',
)
