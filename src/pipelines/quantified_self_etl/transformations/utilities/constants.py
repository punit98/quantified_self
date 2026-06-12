from pyspark.sql import functions as sf
from transformations.utilities import secrets

######### Project Constants #################

dates_to_generate = 28

current_date = sf.current_date()

calendar_start_date = "2023-02-18"

calendar_end_date = sf.date_add(current_date, dates_to_generate)


############ Personal Constants ###############

date_of_birth = secrets.DATE_OF_BIRTH

height_cm = secrets.HEIGHT_CM

EMAIL = secrets.EMAIL
