from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()

fares = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1]) #"/user/hc2660/hw2data/Fares.csv")

licenses = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2]) #"/user/hc2660/hw2data/Licenses.csv")



licenses = licenses.withColumnRenamed('medallion', 'medallion2')

joinExpression = (licenses.medallion2 == fares.medallion)

joined_df = fares.join(licenses, joinExpression, 'inner').drop('medallion2').sort('medallion', 'hack_license', 'pickup_datetime')

joined_df = joined_df.withColumn("pickup_datetime",  F.date_format(F.col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))

joined_df = joined_df.select('medallion', 'hack_license', 'vendor_id', 'pickup_datetime', 'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', F.concat(F.lit('\"'), F.col('name'), F.lit('\"')).alias('name'), 'type', 'current_status', 'DMV_license_plate', 'vehicle_VIN_number', 'vehicle_type', 'model_year', 'medallion_type', 'agent_number', 'agent_name', 'agent_telephone_number', 'agent_website', 'agent_address', 'last_updated_date', 'last_updated_time')

joined_df.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', joined_df.medallion, joined_df.hack_license, joined_df.vendor_id, joined_df.pickup_datetime, joined_df.payment_type, joined_df.fare_amount, joined_df.surcharge, joined_df.mta_tax, joined_df.tip_amount, joined_df.tolls_amount, joined_df.total_amount, joined_df.name, joined_df.type, joined_df.current_status, joined_df.DMV_license_plate, joined_df.vehicle_VIN_number, joined_df.vehicle_type, joined_df.model_year, joined_df.medallion_type, joined_df.agent_number, joined_df.agent_name, joined_df.agent_telephone_number, joined_df.agent_website, joined_df.agent_address, joined_df.last_updated_date, joined_df.last_updated_time)).write.save('task1b-sql.out', format="text")


spark.stop()

