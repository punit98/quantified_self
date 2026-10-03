CREATE OR REFRESH STREAMING TABLE ${raw_schema}.workouts_export
AS
SELECT *
FROM STREAM (
  read_files(
    '/Volumes/${catalog}/${raw_schema}/workouts_export',
    format => 'json',
    multiLine => 'true'
  )
);