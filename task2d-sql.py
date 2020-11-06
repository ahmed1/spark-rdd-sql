from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()


joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c0').alias('medallion'),  F.col('_c3').alias('pickup_datetime') )



medallion_stats = joined_df.withColumn("pickup_datetime",  F.date_format(F.col("pickup_datetime"), "yyyy-MM-dd"))


medallion_stats = medallion_stats.groupBy(F.col('medallion')).agg(F.count('*').alias('total_trips'), F.countDistinct(F.col('pickup_datetime')).alias('days_driven'))

medallion_stats = medallion_stats.select('medallion', 'total_trips', 'days_driven', F.regexp_replace(F.format_number(F.round(F.col('total_trips') / F.col('days_driven'), 2 ), 2), ',', '').alias('average')    ).sort('medallion')


medallion_stats.select(format_string('%s,%s,%s,%s', medallion_stats.medallion, medallion_stats.total_trips, medallion_stats.days_driven, medallion_stats.average)).write.save('task2d-sql.out', format="text")

spark.stop()





