from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()


# using joined_df
joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c15').alias('fare_amount'))

invalid_fare_amounts = joined_df.filter(F.col('fare_amount') < 0).select(F.count('*').alias('invalid_fare_amounts'))


invalid_fare_amounts.select(format_string("%s", invalid_fare_amounts.invalid_fare_amounts)).write.save("task3a-sql.out",format="text")


spark.stop()







