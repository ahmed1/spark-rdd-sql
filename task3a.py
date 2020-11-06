from csv import reader
from pyspark import SparkContext
import sys

sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1) # "/user/as9621/task1a.out")
joined = joined.mapPartitions(lambda x : reader(x))
joined = joined.map(lambda line : [elem.strip() for elem in line])

# x[15] fare_amount
invalid_fares = joined.filter(lambda x : float(x[15]) < 0.0).count()

invalid_fares = sc.parallelize([invalid_fares])

invalid_fares.saveAsTextFile('task3a.out')


sc.stop()



