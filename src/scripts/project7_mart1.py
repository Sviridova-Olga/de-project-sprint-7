import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import when





def main():

    base_events_path = sys.argv[1]
    base_output_path = sys.argv[2]

    conf = SparkConf().setAppName("Project mart - 1")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    df_events_with_city = sql.read.parquet(f'{base_events_path}')

    df_events_with_city_cached = df_events_with_city.cache()

    df_messages = df_events_with_city_cached.where(F.col("event_type") == "message") \
        .selectExpr(
        "event.message_from as user_id",
        "date",
        "event.datetime",
        "lat2",
        "lon2",
        "city",
        "events_id",
        "timezone"
    ).cache()

    window = Window.partitionBy("user_id", "city").orderBy(F.asc("date")).rowsBetween(Window.currentRow,
                                                                                      Window.unboundedFollowing)
    window2 = Window.partitionBy("user_id").orderBy(F.asc("date"))
    window3 = Window.partitionBy("user_id").orderBy(F.asc("date")).rowsBetween(Window.currentRow,
                                                                               Window.unboundedFollowing)
    window4 = Window.partitionBy("user_id").orderBy(F.desc("datetime"))

    mes = df_messages.withColumn('max_date', F.last(F.col("date")).over(window)) \
        .withColumn("city_lag", F.lag("city", 1, "null").over(window2)) \
        .withColumn("act_city", F.last("city").over(window3)) \
        .filter(F.col('city') != F.col('city_lag')) \
        .withColumn("date_lead", F.lead("date", 1, "null").over(window2)) \
        .withColumn("date_r", when(F.col("date_lead").isNotNull(), F.col("date_lead")).otherwise(F.col("max_date"))) \
        .withColumn("date_diff", F.datediff(F.col("date_r"), F.col("date"))) \
        .withColumn('travel_count', F.count("*").over(Window.partitionBy("user_id"))) \
        .select('user_id', 'city', 'act_city', 'travel_count', 'datetime', "date_diff", "timezone", "date")

    cached_mes = mes.cache()

    df_home_city = cached_mes.where(F.col('date_diff') > 26).withColumn("home_city", F.last("city").over(window3)) \
        .select('user_id', 'home_city').distinct()
    df_act_city = cached_mes.groupby('user_id', 'act_city', 'travel_count').agg(
        F.collect_list("city").alias("travel_array")) \
        .select("user_id", 'act_city', 'travel_count', 'travel_array')
    df_local_time = df_messages.where(F.col("datetime").isNotNull()) \
        .withColumn("rn", F.row_number().over(window4)) \
        .filter(F.col("rn") == 1) \
        .withColumn("local_time", F.from_utc_timestamp(F.col("datetime"), F.col('timezone'))) \
        .select("user_id", "local_time")

    df_final = (
        cached_mes.select("user_id").distinct()\
            .join(df_home_city, "user_id", "left")\
            .join(df_act_city, "user_id", "left")\
            .join(df_local_time, "user_id", "left")\
        .select("user_id","act_city","home_city","travel_count","travel_array","local_time")
    ).orderBy('user_id')


    df_final.write.mode("overwrite").parquet(f"{base_output_path}")


if __name__ == "__main__":
    main()
