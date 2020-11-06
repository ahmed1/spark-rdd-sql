# incomplete
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()

joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1])

joined_df = joined_df.select(F.col('_c0').alias('medallion'), F.col('_c1').alias('hack_license'), F.col('_c2').alias('vendor_id'), F.col('_c3').alias('pickup_datetime'), F.col('_c4').alias('rate_code'), F.col('_c5').alias('store_and_fwd_flag'), F.col('_c6').alias('dropoff_datetime'), F.col('_c7').alias('passenger_count'), F.col('_c8').alias('trip_time_in_secs'), F.col('_c9').alias('trip_distance'), F.col('_c10').alias('pickup_longitude'), F.col('_c11').alias('pickup_latitude'), F.col('_c12').alias('dropoff_longitude'), F.col('_c13').alias('dropoff_latitude'), F.col('_c14').alias('payment_type'), F.col('_c15').alias('fare_amount'), F.col('_c16').alias('surcharge'), F.col('_c17').alias('mta_tax'), F.col('_c18').alias('tip_amount'), F.col('_c19').alias('tolls_amount'), F.col('_c20').alias('total_amount'))



left = joined_df.filter(F.col('pickup_longitude') == 0.0).filter(F.col('pickup_latitude') == 0.0).filter(F.col('dropoff_longitude') == 0.0).filter(F.col('dropoff_latitude') == 0.0).select('medallion').groupBy('medallion').agg(F.count('*').alias('all_zero_coordinates'))


right = joined_df.groupBy('medallion').agg(F.count('*').alias('total_taxi_count')).withColumnRenamed('medallion', 'medallion2')

res = left.join(right, left.medallion == right.medallion2).drop('medallion2')

res = res.select('medallion', F.regexp_replace(F.format_number(F.round(100 * F.col('all_zero_coordinates') / F.col('total_taxi_count'), 2), 2), ',','').alias('percentage_of_trips')).sort('medallion')


res.select(format_string('%s,%s', res.medallion, res.percentage_of_trips)).write.save('task3c-sql.out', format="text")


spark.stop()
