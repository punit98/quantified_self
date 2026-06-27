from pyspark import pipelines as dp
from transformations.utilities import paths


@dp.table(
    name=paths.WORKOUT_SET_DETAILS_PATH,
    comment="""The detailed workout table with each set as a separate record
    """,
)
def workout_set_details():
    workout_set_details = spark.readStream.table(paths.INT_WORKOUT_DETAILS_PATH)

    return workout_set_details
