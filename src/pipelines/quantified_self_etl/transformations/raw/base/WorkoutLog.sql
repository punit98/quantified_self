CREATE OR REFRESH STREAMING TABLE ${raw_schema}.WORKOUTLOG
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT *
FROM STREAM read_files(
    '/Volumes/${catalog}/${raw_schema}/landing_zone',
    FORMAT => 'csv',
    HEADER => 'true',
    SCHEMA => 'date_time STRING, muscle_group STRING, exercise STRING, variation STRING, weight STRING, drop_weight STRING, second_drop_weight STRING, reps STRING, drop_reps STRING, second_drop_reps STRING',
    FILENAMEPATTERN => 'WorkoutLog.csv'
)
