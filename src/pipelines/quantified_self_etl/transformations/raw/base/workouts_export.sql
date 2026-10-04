CREATE OR REFRESH STREAMING TABLE ${raw_schema}.workouts_export
AS

with read_files as(

SELECT
  *,
  _metadata.file_path      AS source_file,
  _metadata.file_name      AS source_filename
FROM STREAM read_files(
  '/Volumes/${catalog}/${raw_schema}/workouts_export',
  FORMAT => 'binaryFile'
)
)

SELECT
  source_file,
  CAST(content AS STRING) AS raw_json
  FROM read_files;