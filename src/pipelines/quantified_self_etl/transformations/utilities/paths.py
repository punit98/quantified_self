# Contains the paths for all the source .csv files for the project
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

RAW_SCHEMA = spark.conf.get("raw_schema")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")
GOLD_SCHEMA = spark.conf.get("gold_schema")
SEED_SCHEMA = spark.conf.get("seed_schema")


########### Seed ####################

CALENDAR_PATH = f"{SEED_SCHEMA}.calendar"

CLOCK_PATH = f"{SEED_SCHEMA}.clock"


########### raw ####################

RAW_APPLOG_PATH = f"{RAW_SCHEMA}.applog"

RAW_BODY_MEASUREMENTS_PATH = f"{RAW_SCHEMA}.body_measurements"

RAW_LOCATIONLOG_PATH = f"{RAW_SCHEMA}.locationlog"

RAW_TIMESTAMPLOG_PATH = f"{RAW_SCHEMA}.timestamplog"

RAW_WEATHERLOG_PATH = f"{RAW_SCHEMA}.weatherlog"

RAW_WORKOUTLOG_PATH = f"{RAW_SCHEMA}.workoutlog"


# SOURCE_LOCATION_LOG_PATH =

########### Bronze ####################

STG__APPLOG_PATH = f"{BRONZE_SCHEMA}.stg__applog"

STG__BODY_MEASUREMENTS_PATH = f"{BRONZE_SCHEMA}.stg__body_measurements"

STG__LOCATIONLOG_PATH = f"{BRONZE_SCHEMA}.stg__locationlog"

STG__TIMESTAMPLOG_PATH = f"{BRONZE_SCHEMA}.stg__timestamplog"

STG__WEATHERLOG_PATH = f"{BRONZE_SCHEMA}.stg__weatherlog"

STG__WORKOUTLOG_PATH = f"{BRONZE_SCHEMA}.stg__workoutlog"

############ Silver ####################

INT_WORKOUT_DETAILS = f"{SILVER_SCHEMA}.int_workout_details"


############ Gold ####################

WORKOUT_SET_DETAILS = f"{GOLD_SCHEMA}.workout_set_details"