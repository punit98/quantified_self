from pyspark import pipelines as do
from pyspark.sql import functions as sf
from transformations.utilities import paths, utils
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from functools import partial


int_workout_detail_schema = StructType(
    [
        StructField("date_time", types.TimestampType(), False),
        StructField("utc_offset", types.StringType(), False),
        StructField("calendar_key", types.StringType(), False),
        StructField("clock_key", types.StringType(), False),
        StructField("muscle_group", types.StringType(), False),
        StructField("exercise", types.StringType(), False),
        StructField("variation", types.StringType(), False),
        StructField("weight", types.FloatType(), False),
        StructField("drop_weight", types.FloatType(), True),
        StructField("second_drop_weight", types.FloatType(), True),
        StructField("reps", types.IntegerType(), False),
        StructField("drop_reps", types.IntegerType(), True),
        StructField("second_drop_reps", types.IntegerType(), True),
        StructField("set_volume", types.FloatType(), True),
        StructField("average_weight", types.FloatType(), True),
        StructField("total_reps", types.IntegerType(), True),
        StructField("weight_per_rep", types.FloatType(), True),
        StructField("source", types.StringType(), True),
        StructField("_rescued_data", types.StringType(), False),
    ]
)

ddl_schema = utils.struct_to_ddl(int_workout_detail_schema)


@dp.table(
    name = paths.INT_WORKOUT_DETAILS,
    comment = 
    """The enriched workout detail table with added columns like volume and totals
    """
)
def int_workout_detail()
    workoutlog_df = spark.readStream.table(paths.STG__WORKOUTLOG_PATH)

    weight_columns = [sf.col("weight"), sf.col("drop_weight"), sf.col("second_drop_weight")]
    reps_columns = [sf.col("reps"), sf.col("drop_reps"), sf.col("second_drop_reps")]

    set_volume = (
        (sf.col("weight") * sf.col("reps")) + 
        (sf.col("drop_weight") * sf.col("drop_reps")) + 
        (sf.col("second_drop_weight") * sf.col("second_drop_reps"))
    )
    int_workout_detail = int_workout_detail.withColumn("set_volume", set_volume)

    int_workout_detail = utils.apply_transformation_steps(
        int_workout_detail,
        partial(utils.average_without_zeros, columns_list=weight_columns, output_column_name="average_weight")
    )
    int_workout_detail = int_workout_detail.withColumn("total_reps", sum(rep_col for rep_col in reps_columns))
    int_workout_detail = int_workout_detail.withColumn("weight_per_rep", (sf.col("average_weight") / sf.col("total_reps")))    
    
    
    return int_workout_detail

