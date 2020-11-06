from csv import reader
import sys
from pyspark import SparkContext


sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1) # "/user/as9621/task1a.out")
joined = joined.mapPartitions(lambda x : reader(x))
joined = joined.map(lambda line : [elem.strip() for elem in line])

# medallion -  x[0]
# pickup_datetime - x[3]
joined = joined.map(lambda x : ((x[0], x[3]), (1) )  ).reduceByKey(lambda x, y: (x + y)).filter(lambda x : x[1] > 1).sortByKey().map(lambda x : (x[0][0] + ',' + x[0][1]))

joined.saveAsTextFile('task3b.out')
sc.stop()



from csv import reader
from pyspark import SparkContext
import sys

sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1) # "/user/as9621/task1a.out")
