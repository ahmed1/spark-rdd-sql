from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys
from pyspark import SparkContext
from pyspark.sql import Row


sc = SparkContext()
spark = SparkSession.builder.appName("my_pp").getOrCreate()
# AllTrips is joined_df
joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c15').alias('fare_amount'))


# caching because performing action at each step
joined_df.cache()
# fare_amount


# [] inclusive , () exclusive

# [0, 5] -- 273109
range_0_5 = joined_df.filter(F.col('fare_amount') <= 5).filter(F.col('fare_amount') >= 0).count()


# (5, 15]
range_5_15 = joined_df.filter(F.col('fare_amount') <= 15).filter(F.col('fare_amount') > 5).count()


# (15, 30]
range_15_30 = joined_df.filter(F.col('fare_amount') <= 30).filter(F.col('fare_amount') > 15).count()

# (30, 50]
range_30_50 = joined_df.filter(F.col('fare_amount') <= 50).filter(F.col('fare_amount') > 30).count()

# (50, 100]
range_50_100 = joined_df.filter(F.col('fare_amount') <= 100).filter(F.col('fare_amount') > 50).count()

# [>100)
range_100_plus = joined_df.filter(F.col('fare_amount') > 100).count()


df = sc.parallelize([Row(amount_range='0,5', num_trips=range_0_5), Row(amount_range='5,15', num_trips=range_5_15), Row(amount_range='15,30', num_trips=range_15_30), Row(amount_range='30,50', num_trips=range_30_50), Row(amount_range='50,100', num_trips=range_50_100), Row(amount_range='>100', num_trips= range_100_plus)]).toDF()


df.select(format_string('%s,%s', df.amount_range, df.num_trips)).write.save('task2a-sql.out', format="text")

