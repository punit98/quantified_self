from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from transformations.utilities import constants, paths, utils


@dp.table(
    name=paths.CALENDAR_PATH,
    comment="""
        Calendar table for all dates since constants.TIME_START to current_date + 28 days
        """,
)
def calendar():

    calendar = spark.range(1).withColumn(  # dummy row
        "date",
        sf.explode(
            sf.sequence(
                sf.to_date(sf.lit(constants.CALENDAR_START_DATE), "yyyy-MM-dd"),
                sf.to_date(sf.lit(constants.CALENDAR_END_DATE), "yyyy-MM-dd"),
                sf.expr("interval 1 day"),
            )
        ),
    )
    calendar = utils.create_calendar_key(calendar, "date")

    # Basic components
    calendar = (
        calendar.withColumn("calendar_year", sf.year("date"))
        .withColumn("calendar_month", sf.month("date"))
        .withColumn("calendar_day", sf.dayofmonth("date"))
        .withColumn("day_of_week", sf.date_format("date", "E"))
        .withColumn("day_of_year", sf.dayofyear("date"))
        .withColumn("week_of_year", sf.weekofyear("date"))
        .withColumn("month_name", sf.date_format("date", "MMMM"))
        .withColumn("quarter", sf.quarter("date"))
        .withColumn("date_day", sf.date_format("dd"))
        .withColumn("date_month", sf.date_format("MM"))
    )

    # Flags
    calendar = (
        calendar.withColumn("is_weekend", sf.col("day_of_week").isin("Sat", "Sun"))
        .withColumn("is_month_start", sf.col("calendar_day") == 1)
        .withColumn("is_month_end", sf.col("date") == sf.last_day("date"))
        .withColumn("is_quarter_start", sf.expr("date = trunc(date, 'quarter')"))
        .withColumn(
            "is_quarter_end",
            sf.expr("date = add_months(trunc(date, 'quarter'), 3) - interval 1 day"),
        )
        .withColumn("is_year_start", sf.date_format("date", "MM-dd") == sf.lit("01-01"))
        .withColumn("is_year_end", sf.date_format("date", "MM-dd") == sf.lit("12-31"))
    )

    # Week start/end (ISO week: Monday = 1)
    calendar = calendar.withColumn(
        "week_start_date", sf.date_sub("date", sf.dayofweek("date") - 2)
    ).withColumn("week_end_date", sf.date_add("week_start_date", 6))

    # Next/previous dates
    calendar = (
        calendar.withColumn("next_day", sf.date_add("date", 1))
        .withColumn("prev_day", sf.date_add("date", -1))
        .withColumn("next_week", sf.date_add("date", 7))
        .withColumn("prev_week", sf.date_add("date", -7))
        .withColumn("next_month", sf.add_months("date", 1))
        .withColumn("prev_month", sf.add_months("date", -1))
        .withColumn("next_quarter", sf.add_months("date", 3))
        .withColumn("prev_quarter", sf.add_months("date", -3))
        .withColumn("next_year", sf.add_months("date", 12))
        .withColumn("prev_year", sf.add_months("date", -12))
    )

    # Current/past/future flags
    calendar = (
        calendar.withColumn("is_current_day", sf.col("date") == constants.CURRENT_DATE)
        .withColumn("is_past_day", sf.col("date") < constants.CURRENT_DATE)
        .withColumn("is_future_day", sf.col("date") > constants.CURRENT_DATE)
    )

    # Age calculations
    calendar = (
        calendar.withColumn(
            "age_years", sf.floor(sf.months_between("date", sf.lit(constants.DATE_OF_BIRTH)) / 12)
        )
        .withColumn(
            "age_months", sf.floor(sf.months_between("date", sf.lit(constants.DATE_OF_BIRTH)))
        )
        .withColumn("age_days", sf.datediff("date", sf.lit(constants.DATE_OF_BIRTH)))
    )

    return calendar
