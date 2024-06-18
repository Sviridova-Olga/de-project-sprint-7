import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.sql.functions import when
from math import radians, cos, sin, asin, sqrt
from pytz import country_timezones


def haversine(lat1, lon1, lat2, lon2):
    R = 6372
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    a = sin(dLat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dLon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return R * c



def main():

    base_events_path = sys.argv[1]
    base_output_path = sys.argv[2]

    conf = SparkConf().setAppName("Project mart - 3")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    haversine_udf = udf(haversine)

    df_events_with_city = sql.read.parquet(f'{base_events_path}')

    df_events_with_city_cached = df_events_with_city.cache()

    df_subscriptions = (
        df_events_with_city_cached.where(
            (F.col("event_type") == "subscription") & (F.col("event.subscription_channel").isNotNull())) \
            .select(F.col("event.subscription_channel").alias("channel_id"),F.col("event.user").alias("user_id"))
    )

    df1 = df_subscriptions.alias("df1")
    df2 = df_subscriptions.alias("df2")

    df_users_with_same_channels = df1.join(df2, on="channel_id", how="inner") \
        .where(F.col("df1.user_id") != F.col("df2.user_id")) \
        .select(F.least(F.col("df1.user_id").cast("int"), F.col("df2.user_id").cast("int")).alias("user_left"), \
                F.greatest(F.col("df1.user_id").cast("int"), F.col("df2.user_id").cast("int")).alias("user_right")) \
        .distinct()

    df_messages = (
        df_events_with_city_cached.filter(F.col("event_type") == "message") \
            .where(F.col("event.message_from").isNotNull() & F.col("event.message_to").isNotNull()) \
            .select(F.col("event.message_from").alias("message_from"), F.col("event.message_to").alias("message_to"))) \
        .distinct() \
        .select(F.least(F.col("message_from").cast("int"), F.col("message_to").cast("int")).alias("user_left"), \
                F.greatest(F.col("message_from").cast("int"), F.col("message_to").cast("int")).alias("user_right")) \
        .distinct()

    df_users = df_users_with_same_channels.join(df_messages, on=["user_left", "user_right"], how="left_anti")


    window4 = Window.partitionBy("user_id").orderBy(F.desc("date"), F.desc("datetime"))
    df_local_time = df_events_with_city_cached.filter(F.col("event_type") == "message") \
        .where((F.col("event.message_from").isNotNull()) & (F.col("lat").isNotNull()) & (F.col("lon").isNotNull())) \
        .select(F.col("event.message_from").alias("user_id"), "date", "lon", "lat", "city", "timezone",
                F.col("event.datetime").alias("datetime")) \
        .withColumn("rn", F.row_number().over(window4)) \
        .withColumn("local_time", F.from_utc_timestamp(F.coalesce(F.col("datetime"), F.to_timestamp(F.col('date'))),
                                                       F.col('timezone'))) \
        .filter(F.col("rn") == 1) \
        .select("user_id", "lat", "lon", "city", "local_time")


    df_final_mart3 = df_users.join(df_local_time, df_users.user_left == df_local_time.user_id, how="inner") \
        .select("user_left", "user_right", F.col("lat").alias("lat_left"), F.col("lon").alias("lon_left"),
                F.col("local_time").alias("local_time_left")) \
        .join(df_local_time, df_users.user_right == df_local_time.user_id, how="inner") \
        .select("user_left", "user_right", "lat_left", "lon_left", F.col("lat").alias("lat_right"),
                F.col("lon").alias("lon_right"), "city", "local_time_left") \
        .withColumn('dst',
        haversine_udf(F.col('lat_left'), F.col('lon_left'), F.col('lat_right'), F.col('lon_right')).cast("float"))\
        .filter(F.col("dst") <= 1.0) \
        .withColumn("processed_dttm", F.current_timestamp()) \
        .withColumnRenamed("local_time_left", "local_time") \
        .select("user_left", "user_right", "processed_dttm", F.col("city").alias("zone_id"), "local_time")


    df_final_mart3.write.mode("overwrite").parquet(f"{base_output_path}")


if __name__ == "__main__":
    main()
