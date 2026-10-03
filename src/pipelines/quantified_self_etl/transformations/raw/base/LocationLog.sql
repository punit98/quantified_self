CREATE OR REFRESH STREAMING TABLE ${raw_schema}.LOCATIONLOG
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT *
FROM STREAM read_files(
    '/Volumes/${catalog}/${raw_schema}/landing_zone',
    FORMAT => 'csv',
    HEADER => 'true',
    SCHEMA => 'timestamp STRING, latitude STRING, longitude STRING',
    FILENAMEPATTERN => 'LocationLog.csv'
)
