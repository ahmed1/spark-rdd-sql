from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("my_pp").getOrCreate()

joined_df = spark.read.format('csv').options(header = 'false', inferschema = 'true').load(sys.argv[1]).select(F.col('_c3').alias('pickup_datetime'), F.col('_c15').alias('fare_amount'), F.col('_c16').alias('surcharge'),  F.col('_c18').alias('tip_amount'), F.col('_c19').alias('tolls_amount'), )
 
 
total_rev_df_add = joined_df.select(F.col('pickup_datetime'), (F.col('fare_amount') + F.col('surcharge') + F.col('tip_amount')).alias('total_revenue'), F.col('tolls_amount'))




total_rev_df_add = total_rev_df_add.withColumn('pickup_datetime',  F.date_format(F.col('pickup_datetime'), 'yyyy-MM-dd'))



total_rev_grouped = total_rev_df_add.groupBy(F.col('pickup_datetime').alias('date')).agg(F.sum('total_revenue').alias('total_revenue'), F.sum('tolls_amount').alias('total_tolls'))




total_rev_grouped = total_rev_grouped.select('date', F.regexp_replace(F.format_number(F.round(F.col('total_revenue'), 2), 2), ',', '').alias('total_revenue'), F.regexp_replace(F.format_number(F.round(F.col('total_tolls'), 2), 2), ',', '').alias('total_tolls') )

total_rev_grouped = total_rev_grouped.sort('date')

total_rev_grouped.select(format_string('%s,%s,%s', total_rev_grouped.date, total_rev_grouped.total_revenue, total_rev_grouped.total_tolls)).write.save('task2c-sql.out', format="text")


spark.stop()

