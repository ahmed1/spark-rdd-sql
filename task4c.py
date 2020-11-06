from csv import reader
import sys
from pyspark import SparkContext


sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1) # "/user/as9621/task1b.out")
joined = joined.mapPartitions(lambda x : reader(x))
joined = joined.map(lambda line : [elem.strip() for elem in line])


# agent_name x[20] | fare_amount x[5]


joined = joined.map(lambda x : ( (x[20]), (float(x[5]))  )).reduceByKey(lambda x, y : (x + y) ).sortBy(lambda x : x[1], ascending =  False)

joined = sc.parallelize(joined.take(10))

joined = joined.map(lambda x : str(x[0]) + ',' + str(format(round(x[1], 2), '.2f'))     )

joined.saveAsTextFile('task4c.out')
sc.stop()
