# may be able to get this in rdd
import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
joined = sc.textFile(sys.argv[1], 1) # "/user/as9621/task1a.out")
joined = joined.mapPartitions(lambda x : reader(x))
joined = joined.map(lambda line : [elem.strip() for elem in line])



# medallion x[0] -- only key
# pickup_longitude x[10] -> x[13]

left = joined.map(lambda x : (x[0], 1)  ).reduceByKey(lambda x, y: (x + y)) # count each medallion

right = joined.filter(lambda x : ( (float(x[10]) == 0.0) & (float(x[11]) == 0.0) & (float(x[12]) == 0.0) & (float(x[13]) == 0.0)   )).map(lambda x : (x[0], 1)  ).reduceByKey(lambda x, y: (x + y)) # get count per medallion once apply filter


res = left.join(right)

res = res.sortByKey()

res = res.map(lambda x : (x[0], round((x[1][1]/(x[1][1] + x[1][0])), 2) )  ) 

res = res.map(lambda x : x[0] + ',' + format(x[1], '.2f'))




res.saveAsTextFile('task3c.out')
sc.stop()
