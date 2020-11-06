from pyspark import SparkContext
from csv import reader
import sys
from operator import add


sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1) # "/user/as9621/task1a.out")
joined = joined.mapPartitions(lambda x : reader(x))
joined = joined.map(lambda line : [elem.strip() for elem in line])


left = joined.map(lambda x : (x[0], 1)    ).reduceByKey(lambda x, y : (x + y) ) # total_trips

right = joined.map(lambda x : (    (x[0], x[3][:10]), (1) )    ).reduceByKey(lambda x, y : (x + y) ).map(lambda x :  (x[0][0], 1 ) ).reduceByKey(lambda x, y : (x + y))

joined1 = left.join(right).sortByKey()


joined1 = joined1.map(lambda x :  (x[0], x[1][0], x[1][1],  round(x[1][0] / x[1][1], 2) ) )

# not sure if needed
joined1 = joined1.map(lambda x : x[0] + ',' + str(x[1]) + ',' + str(x[2]) + ',' + format(x[3], '.2f'))

joined1.saveAsTextFile('task2d.out')
sc.stop()


