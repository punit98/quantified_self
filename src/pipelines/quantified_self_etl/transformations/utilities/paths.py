#Contains the paths for all the source .csv files for the project
import pandas as pd
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

RAW_SCHEMA = spark.conf.get("raw_schema")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
SILVER_SCHEMA = spark.conf.get("silver__schema")
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

BASE__APPLOG_PATH = f"{BRONZE_SCHEMA}.base__applog"

BASE__BODY_MEASUREMENTS_PATH = f"{BRONZE_SCHEMA}.base__body_measurements"

BASE__LOCATIONLOG_PATH = f"{BRONZE_SCHEMA}.base__locationlog"

BASE__TIMESTAMPLOG_PATH = f"{BRONZE_SCHEMA}.base__timestamplog"

BASE__WEATHERLOG_PATH = f"{BRONZE_SCHEMA}.base__weatherlog"

BASE__WORKOUTLOG_PATH = f"{BRONZE_SCHEMA}.base__workoutlog"

############ Silver ####################




############ Gold ####################


