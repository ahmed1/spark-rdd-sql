from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()

# 59 total agents

joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c20').alias('agent_name'), F.col('_c5').alias('fare_amount'))




res = joined_df.groupBy('agent_name').agg(F.sum('fare_amount').alias('total_revenue') ).sort('total_revenue', ascending = False)


res = res.limit(10)


res = res.select('agent_name', F.regexp_replace(F.format_number(F.round(F.col('total_revenue'), 2), 2), ',','').alias('total_revenue') )


res.select(format_string('%s,%s', res.agent_name, res.total_revenue)).write.save('task4c-sql.out', format="text")

spark.stop()

