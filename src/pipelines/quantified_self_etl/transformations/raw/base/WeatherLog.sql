CREATE OR REFRESH STREAMING TABLE ${raw_schema}.WEATHERLOG
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT *
FROM STREAM read_files(
    '/Volumes/${catalog}/${raw_schema}/landing_zone',
    FORMAT => 'csv',
    HEADER => 'true',
    INFERSCHEMA => 'false',
    FILENAMEPATTERN => 'WeatherLog.csv'
)
