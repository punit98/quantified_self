import pandas as pd
from pyspark.sql import functions as sf




######### Project Constatns #################

dates_to_generate = 28

current_date = sf.current_date()

calendar_start_date = "2023-02-18"

calendar_end_date = sf.date_add(current_date, dates_to_generate)



############ Personal Constatns ###############

date_of_birth = dbutils.secrets.get("personal-details", "date_of_birth")

heaight_cm = dbutils.secrets.get("personal-details", "heaight_cm")

