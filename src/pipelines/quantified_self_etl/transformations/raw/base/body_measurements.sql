CREATE OR REFRESH STREAMING TABLE ${raw_schema}.body_measurements
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT *
FROM STREAM read_files(
    '/Volumes/${catalog}/${raw_schema}/landing_zone',
    format => 'csv',
    header => 'true',
    schema => 'date_time STRING, is_pump STRING, chest STRING, 
    shoulder STRING, left_bicep STRING, left_forearm STRING, right_bicep STRING, 
    right_forearm STRING, waist STRING, tond STRING, ass STRING, left_thigh STRING, 
    left_calf STRING, right_thigh STRING, right_calf STRING, 
    weight STRING, height STRING, neck STRING',
    filenamepattern => 'body_measurements.csv'
)
