from csv import reader
import sys
from pyspark import SparkContext


sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1)  # "/user/as9621/task1b.out")
joined = joined.mapPartitions(lambda x : reader(x))
joined = joined.map(lambda line : [elem.strip() for elem in line])


# medallion type x[18], fare_amount x[5], tip_amount x[8]

left = joined.map(lambda x : (x[18], 1)).reduceByKey(lambda x, y : (x + y))


right = joined.map(lambda x : (  ((x[18]) , (float(x[5]), float(x[8]) / float(x[5]))  ) if float(x[5]) != 0.0 else ((x[18]) , (float(x[5]), 0.0)  ) ) ).reduceByKey(lambda x, y :   (x[0]+y[0], x[1]+y[1]   )   )

joined = left.join(right).sortByKey()

joined = joined.map(lambda x : str(x[0]) + ',' + str(x[1][0]) + ',' + str(format(round(x[1][1][0], 2), '.2f')) + ',' + str(format(round(x[1][1][1] / x[1][0] * 100, 2), '.2f'))     )


joined.saveAsTextFile('task4b.out')
sc.stop()
