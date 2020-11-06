from csv import reader
import sys
from pyspark import SparkContext


sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1)  # "/user/as9621/task1a.out")
joined = joined.mapPartitions(lambda x : reader(x))
joined = joined.map(lambda line : [elem.strip() for elem in line])


joined = joined.map(lambda x : (    (x[1], x[0]), (1) )    ).reduceByKey(lambda x, y : (x + y) ).map(lambda x :  (x[0][0], 1 ) ).reduceByKey(lambda x, y : (x + y)).sortByKey()

joined = joined.map(lambda x : str(x[0]) + ',' + str(x[1]))


joined.saveAsTextFile('task3d.out')
sc.stop()

