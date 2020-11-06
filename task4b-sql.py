from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()

# medallion_type _c18, fare_amount _c5, tip_amount _c8

joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c18').alias('medallion_type'), F.col('_c5').alias('fare_amount'), F.col('_c8').alias('tip_amount'), (F.col('_c8') / F.col('_c5')).alias('tip_percentage')   )



res = joined_df.groupBy('medallion_type').agg(F.count('*').alias('total_trips'),
F.regexp_replace(F.format_number(F.round(F.sum('fare_amount'), 2),2),',','').alias('total_revenue'), F.regexp_replace(F.format_number(F.round(100 * F.sum('tip_percentage') / F.count('*'),2),2),',','').alias('avg_tip_percentage')).sort('medallion_type')




res.select(format_string('%s,%s,%s,%s', res.medallion_type, res.total_trips, res.total_revenue, res.avg_tip_percentage)).write.save('task4b-sql.out', format="text")

spark.stop()
