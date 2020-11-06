from csv import reader
import sys
from pyspark import SparkContext
from operator import add

sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1) # "/user/as9621/task1a.out")
joined = joined.mapPartitions(lambda x : reader(x)) # no header here


joined = joined.map(lambda x : (x[7], 1) ).reduceByKey(add).sortByKey()

joined = joined.map(lambda x : x[0] + ',' + str(x[1]) )

joined.saveAsTextFile('task2b.out')

sc.stop()
