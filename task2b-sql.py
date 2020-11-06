from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()

joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c7').alias('passenger_count'))




joined_df = joined_df.withColumn('passenger_count', joined_df.passenger_count.cast(IntegerType()))

num_pass_dist = joined_df.groupBy(F.col('passenger_count').alias('num_of_passengers')).agg(F.count('*').alias('num_trips')).sort('num_of_passengers')


num_pass_dist.select(format_string('%s,%s', num_pass_dist.num_of_passengers, num_pass_dist.num_trips)).write.save('task2b-sql.out', format="text")



spark.stop()
