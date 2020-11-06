from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()

trips = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1]) #"/user/hc2660/hw2data/Trips.csv"

fares = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2]) #"/user/hc2660/hw2data/Fares.csv"\


# rename fares columns
trips = trips.withColumnRenamed('medallion', 'medallion2').withColumnRenamed('hack_license', 'hack_license2').withColumnRenamed('vendor_id', 'vendor_id2').withColumnRenamed('pickup_datetime', 'pickup_datetime2')


# joinExpression
joinExpression = ( (trips.medallion2 == fares.medallion) & (trips.hack_license2 == fares.hack_license) & (trips.vendor_id2 == fares.vendor_id) & (trips.pickup_datetime2 == fares.pickup_datetime)  )


# inner join

joined_df = fares.join(trips, joinExpression, 'inner').drop('medallion2', 'hack_license2', 'vendor_id2', 'pickup_datetime2')



joined_df = joined_df.sort('medallion', 'hack_license', 'pickup_datetime')





joined_df = joined_df.select('medallion', 'hack_license', 'vendor_id', 'pickup_datetime', 'rate_code', 'store_and_fwd_flag', 'dropoff_datetime', 'passenger_count', 'trip_time_in_secs', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount')


joined_df = joined_df.withColumn("pickup_datetime",  F.date_format(F.col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")).withColumn("dropoff_datetime",  F.date_format(F.col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))


joined_df.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', joined_df.medallion, joined_df.hack_license, joined_df.vendor_id, joined_df.pickup_datetime, joined_df.rate_code, joined_df.store_and_fwd_flag, joined_df.dropoff_datetime, joined_df.passenger_count, joined_df.trip_time_in_secs, joined_df.trip_distance, joined_df.pickup_longitude, joined_df.pickup_latitude, joined_df.dropoff_longitude, joined_df.dropoff_latitude, joined_df.payment_type, joined_df.fare_amount, joined_df.surcharge, joined_df.mta_tax, joined_df.tip_amount, joined_df.tolls_amount, joined_df.total_amount)).write.save('task1a-sql.out', format="text")



spark.stop()


