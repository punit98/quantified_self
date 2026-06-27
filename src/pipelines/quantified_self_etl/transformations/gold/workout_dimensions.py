from pyspark import pipelines as dp
from transformations.utilities import paths


@dp.table(
    name=paths.DIM_MUSCLE_GROUPS_PATH,
    comment="""
    Unique muscle Groups from the workout_detail table
    """,
)
def dim_muscle_groups():
    workout_detail = spark.readStream.table(paths.INT_WORKOUT_DETAILS_PATH)
    muscle_groups = workout_detail.select("muscle_group").distinct()
    return muscle_groups


@dp.table(
    name=paths.DIM_EXERCISES_PATH,
    comment="""
    Unique exercises for muscle groups from the workout_detail table
    """,
)
def dim_exercises():
    workout_detail = spark.readStream.table(paths.INT_WORKOUT_DETAILS_PATH)
    exercises = workout_detail.select("exercise").distinct()
    return exercises


@dp.table(
    name=paths.DIM_VARIATIONS_PATH,
    comment="""
    Unique variations of exercises from the workout_detail table
    """,
)
def dim_variations():
    workout_detail = spark.readStream.table(paths.INT_WORKOUT_DETAILS_PATH)
    variations = workout_detail.select("variation").distinct()
    return variations
