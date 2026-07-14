from functools import partial

from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from transformations.utilities import paths, utils


int_body_measurements_schema = StructType(
    [
        StructField("date_time", types.TimestampType(), False),
        StructField("utc_offset", types.StringType(), False),
    ]
)

ddl_schema = utils.struct_to_ddl(int_body_measurements_schema)


