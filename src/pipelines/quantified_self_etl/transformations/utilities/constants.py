from pyspark.sql import functions as sf
from transformations.utilities import secrets

######### Project Constants #################

DATES_TO_GENERATE = 28

CURRENT_DATE = sf.current_date()

CALENDAR_START_DATE = "2023-02-18"

CALENDAR_END_DATE = sf.date_add(CURRENT_DATE, DATES_TO_GENERATE)




############ Personal Constants ###############

date_of_birth = secrets.DATE_OF_BIRTH

height_cm = secrets.HEIGHT_CM

EMAIL = secrets.EMAIL
