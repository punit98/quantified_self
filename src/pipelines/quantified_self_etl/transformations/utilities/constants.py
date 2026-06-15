from pyspark.sql import functions as sf
from transformations.utilities import secrets
from datetime import datetime, timedelta

######### Project Constants #################

FUTURE_DATES_TO_GENERATE = 28

CURRENT_DATE = datetime.today().date()

CALENDAR_START_DATE = "2023-02-18"

CALENDAR_END_DATE = datetime.today().date() + timedelta(days=FUTURE_DATES_TO_GENERATE)



############ Personal Constants ###############

DATE_OF_BIRTH = secrets.DATE_OF_BIRTH

HEIGHT_CM = secrets.HEIGHT_CM

EMAIL = secrets.EMAIL

