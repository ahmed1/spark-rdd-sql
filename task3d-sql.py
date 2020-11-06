from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()


joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c0').alias('medallion'), F.col('_c1').alias('hack_license'))

res = joined_df.groupBy('hack_license').agg(F.countDistinct('medallion').alias('num_taxis_used')).sort('hack_license')

res.select(format_string('%s,%s', res.hack_license, res.num_taxis_used)).write.save('task3d-sql.out', format="text")

spark.stop()

