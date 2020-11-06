from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()


joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c0').alias('medallion'), F.col('_c3').alias('pickup_datetime'))

joined_df = joined_df.withColumn("pickup_datetime",  F.date_format(F.col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))

records_repeated = joined_df.groupBy('medallion', 'pickup_datetime').agg(F.count('*').alias('count')).filter(F.col('count') > 1).sort('medallion', 'pickup_datetime').select('medallion', 'pickup_datetime')

records_repeated.select(format_string('%s,%s', records_repeated.medallion, records_repeated.pickup_datetime)).write.save('task3b-sql.out', format="text")

spark.stop()





