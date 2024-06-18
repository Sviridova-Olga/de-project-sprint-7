import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
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


def timezone_helper(city_name: str):
    if f'Australia/{city_name}' in country_timezones['AU']:
        return f'Australia/{city_name}'
    else:
        return 'Australia/Sydney'



def main():

    base_events_path = sys.argv[1]
    base_geo_path = sys.argv[2]
    base_output_path = sys.argv[3]

    conf = SparkConf().setAppName("Project7 init")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    haversine_udf = udf(haversine)
    timezone_helper_udf = udf(timezone_helper)

    #geo_events = sql.read.parquet('/user/master/data/geo/events').sample(0.1)
    #geo_events.write.partitionBy(['event_type', 'date']) \
        #.mode("overwrite").parquet('/user/sviridovao/data/geo_events')


    events = sql.read.parquet(f'{base_events_path}') \
        .where("lat is not null and lon is not null") \
        .select(F.monotonically_increasing_id().alias('events_id'), '*')
    geo = sql.read.csv(f'{base_geo_path}', sep=";", header=True)
    geo_new = geo.select('id', 'city', F.regexp_replace(F.col('lat'), ',', '.').cast(DoubleType()).alias('lat2'),
                         F.regexp_replace(F.col('lng'), ',', '.').cast(DoubleType()).alias('lon2'))

    window = Window.partitionBy("events_id").orderBy("dst")
    df_events_geo = events.crossJoin(geo_new) \
        .withColumn('dst', haversine_udf(F.col('lat2'), F.col('lon2'), F.col('lat'), F.col('lon')).cast("float"))

    df_events_with_city = df_events_geo \
        .withColumn('rn', F.row_number().over(window)) \
        .withColumn('timezone', timezone_helper_udf(F.col("city")).cast("string")) \
        .filter(F.col('rn') == 1)


    df_events_with_city.write.mode("overwrite").parquet(f"{base_output_path}")


if __name__ == "__main__":
    main()
