from csv import reader
import sys
from pyspark import SparkContext

sc = SparkContext()

rdd_fares = sc.textFile(sys.argv[1], 1) # "/user/hc2660/hw2data/Fares.csv")
rdd_fares = rdd_fares.mapPartitions(lambda x : reader(x))

rdd_licenses = sc.textFile(sys.argv[2], 1) # "/user/hc2660/hw2data/Licenses.csv")
rdd_licenses = rdd_licenses.mapPartitions(lambda x : reader(x))

rdd_fares = rdd_fares.filter(lambda x : "medallion" not in x)
rdd_licenses = rdd_licenses.filter(lambda x : "medallion" not in x)

rdd_fares = rdd_fares.map(lambda x : ((x[0]), (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10]) ) )
rdd_licenses = rdd_licenses.map(lambda x : ((x[0]), (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15]) ) )

joined = rdd_fares.join(rdd_licenses)

joined = joined.sortBy(lambda x : (x[0], x[1][0][0], x[1][0][2]))

joined = joined.map(lambda x : x[0] + ',' + ','.join(val_left for val_left in x[1][0]) + ',' + '\"' + x[1][1][0] + '\"' + ',' + ','.join(val_right for val_right in x[1][1][1:]))

joined.saveAsTextFile('task1b.out')

sc.stop()
