import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window



def main():

    base_events_path = sys.argv[1]
    base_output_path = sys.argv[2]

    conf = SparkConf().setAppName("Project mart - 2")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    df_events_with_city = sql.read.parquet(f'{base_events_path}')

    df_events_with_city_cached = df_events_with_city.cache()

    df_with_date = df_events_with_city_cached.withColumn('month', F.trunc(F.col('date'), 'month')) \
        .withColumn('week', F.trunc(F.col('date'), 'week')) \
        .orderBy('city', 'events_id') \
        .select('events_id', 'event_type', 'date', 'city', 'month', 'week',
                F.col('event.message_from').alias('user_id'))

    df_with_date_cached = df_with_date.cache()

    window = Window.partitionBy("user_id").orderBy("date")
    window2 = Window.partitionBy("city", "month").orderBy("month")
    window3 = Window.partitionBy("city", "week").orderBy("week")

    df_messages = df_with_date_cached.where(F.col("event_type") == "message") \
        .withColumn('rn', F.row_number().over(window)) \
        .withColumn('reg', F.when(F.col('rn') == 1, 1).otherwise(0)) \
        .withColumn('month_message', F.count('events_id').over(window2)) \
        .withColumn('week_message', F.count('events_id').over(window3)) \
        .withColumn('month_user', F.sum('reg').over(window2)) \
        .withColumn('week_user', F.sum('reg').over(window3)) \
        .select('month', 'week', 'city', 'week_message', 'month_message', 'week_user', 'month_user') \
        .distinct()

    df_subscription = df_with_date_cached.where(F.col("event_type") == "subscription") \
        .withColumn('month_subscription', F.count('events_id').over(window2)) \
        .withColumn('week_subscription', F.count('events_id').over(window3)) \
        .select('month', 'week', 'city', 'month_subscription', 'week_subscription') \
        .distinct()

    df_reaction = df_with_date_cached.where(F.col("event_type") == "reaction") \
        .withColumn('month_reaction', F.count('events_id').over(window2)) \
        .withColumn('week_reaction', F.count('events_id').over(window3)) \
        .select('month', 'week', 'city', 'month_reaction', 'week_reaction') \
        .distinct()

    df_final_mart2 = (df_messages.join(df_subscription, ["month", "week", "city"], "full")\
                      .join(df_reaction, ["month", "week", "city"], "full")\
                      .select('month', 'week', 'city', 'week_message', 'month_message', 'week_user', 'month_user',
                              'month_subscription', 'week_subscription', 'month_reaction', 'week_reaction')) \
        .na.fill(value=0)

    df_final_mart2.write.mode("overwrite").parquet(f"{base_output_path}")


if __name__ == "__main__":
    main()
