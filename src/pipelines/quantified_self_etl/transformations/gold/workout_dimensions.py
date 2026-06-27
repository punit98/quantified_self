from functools import partial

from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils



@dp.table(
    name = paths.DIM_MUSCLE_GROUPS_PATH,
    comment =
    """
    Unique muscle Groups from the workout_detail table
    """
)
def dim_muscle_groups():
    workout_detail = spark.readStream.table(paths.INT_WORKOUT_DETAILS_PATH)
    muscle_groups = workout_detail.select("muscle_group").distinct()
    return muscle_groups