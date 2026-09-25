from functools import partial

from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils

int_body_measurements_schema = StructType(
    [
        # Passthrough / source columns
        StructField("date_time", types.TimestampType(), False),
        StructField("utc_offset", types.StringType(), False),
        StructField("calendar_key", types.StringType(), False),
        StructField("clock_key", types.StringType(), False),
        StructField("is_pump", types.StringType(), False),
        StructField("chest", types.FloatType(), False),
        StructField("shoulder", types.FloatType(), False),
        StructField("left_bicep", types.FloatType(), False),
        StructField("left_forearm", types.FloatType(), False),
        StructField("right_bicep", types.FloatType(), False),
        StructField("right_forearm", types.FloatType(), False),
        StructField("waist", types.FloatType(), False),
        StructField("belly", types.FloatType(), False),  # renamed from "tond"
        StructField("glutes", types.FloatType(), False),  # renamed from "ass"
        StructField("left_thigh", types.FloatType(), False),
        StructField("left_calf", types.FloatType(), False),
        StructField("right_thigh", types.FloatType(), False),
        StructField("right_calf", types.FloatType(), False),
        StructField("weight", types.FloatType(), False),
        StructField("height", types.FloatType(), False),
        StructField("neck", types.FloatType(), True),  # blank in early records
        # Enriched / derived columns
        StructField("bmi", types.FloatType(), True),
        StructField("waist_to_hip_ratio", types.FloatType(), True),
        StructField("waist_to_chest_ratio", types.FloatType(), True),
        StructField("waist_to_shoulder_ratio", types.FloatType(), True),
        StructField("chest_to_hip_ratio", types.FloatType(), True),
        StructField("waist_to_height_ratio", types.FloatType(), True),
        StructField("neck_to_waist_ratio", types.FloatType(), True),
        StructField("bicep_symmetry_ratio", types.FloatType(), True),
        StructField("forearm_symmetry_ratio", types.FloatType(), True),
        StructField("thigh_symmetry_ratio", types.FloatType(), True),
        StructField("calf_symmetry_ratio", types.FloatType(), True),
        StructField("left_bicep_to_forearm_ratio", types.FloatType(), True),
        StructField("right_bicep_to_forearm_ratio", types.FloatType(), True),
        StructField("left_thigh_to_calf_ratio", types.FloatType(), True),
        StructField("right_thigh_to_calf_ratio", types.FloatType(), True),
        StructField("source", types.StringType(), True),
        StructField("_rescued_data", types.StringType(), False),
    ]
)

ddl_schema = utils.struct_to_ddl(int_body_measurements_schema)


def _safe_ratio(numerator_col, denominator_col):
    """Divide two columns, returning null instead of raising on divide-by-zero/null."""
    return sf.when(
        (sf.col(denominator_col).isNotNull()) & (sf.col(denominator_col) != 0),
        sf.col(numerator_col) / sf.col(denominator_col),
    ).otherwise(sf.lit(None).cast(types.FloatType()))


@dp.table(
    name=paths.INT_BODY_MEASUREMENTS_PATH,
    comment="""The enriched body measurement table with BMI and body-proportion ratios
    """,
)
def int_body_measurements():
    # NOTE: update this to your actual STG constant name if it differs
    bodymeasurements_df = spark.readStream.table(paths.STG__BODYMEASUREMENTS_PATH)

    int_body_measurements = bodymeasurements_df.withColumnRenamed(
        "tond", "belly"
    ).withColumnRenamed("ass", "glutes")

    # Normalize is_pump casing (source has "Pump" / "No Pump" / "No pump")
    int_body_measurements = int_body_measurements.withColumn(
        "is_pump",
        sf.when(sf.lower(sf.trim(sf.col("is_pump"))) == "pump", sf.lit("Pump")).otherwise(
            sf.lit("No Pump")
        ),
    )

    # BMI: weight (kg) / height (m)^2
    int_body_measurements = int_body_measurements.withColumn(
        "bmi", sf.col("weight") / sf.pow(sf.col("height") / 100, 2)
    )

    # Body-proportion ratios
    ratio_specs = {
        "waist_to_hip_ratio": ("waist", "glutes"),
        "waist_to_chest_ratio": ("waist", "chest"),
        "waist_to_shoulder_ratio": ("waist", "shoulder"),
        "chest_to_hip_ratio": ("chest", "glutes"),
        "waist_to_height_ratio": ("waist", "height"),
        "neck_to_waist_ratio": ("neck", "waist"),
        "bicep_symmetry_ratio": ("left_bicep", "right_bicep"),
        "forearm_symmetry_ratio": ("left_forearm", "right_forearm"),
        "thigh_symmetry_ratio": ("left_thigh", "right_thigh"),
        "calf_symmetry_ratio": ("left_calf", "right_calf"),
        "left_bicep_to_forearm_ratio": ("left_bicep", "left_forearm"),
        "right_bicep_to_forearm_ratio": ("right_bicep", "right_forearm"),
        "left_thigh_to_calf_ratio": ("left_thigh", "left_calf"),
        "right_thigh_to_calf_ratio": ("right_thigh", "right_calf"),
    }

    for output_col, (numerator_col, denominator_col) in ratio_specs.items():
        int_body_measurements = int_body_measurements.withColumn(
            output_col, _safe_ratio(numerator_col, denominator_col)
        )

    return int_body_measurements