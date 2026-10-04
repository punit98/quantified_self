create or refresh streaming table ${raw_schema}.workouts_export
as

with read_files as (

    select *
    from stream read_files(
        '/Volumes/${catalog}/${raw_schema}/workouts_export',
        format => 'binaryfile'
    )
)

select cast(content as string) as raw_json
from read_files;
