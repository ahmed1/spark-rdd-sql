from csv import reader
import sys
from pyspark import SparkContext


sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1)
joined = joined.mapPartitions(lambda x : reader(x)) # no header here

# fare amount - 15
# tolls_amount 19
joined = joined.map(lambda x : [elem.strip() for elem in x]).map(lambda x : (   (x[3][:10]), (round(float(x[15].strip()) + float(x[18].strip()) + float(x[16].strip()), 2), float(x[19].strip())   )    )      )

# reduceByKey lambda refers to the values
joined = joined.reduceByKey(lambda x, y: (round(x[0]+y[0], 2), round(x[1]+y[1], 2)   )   ).sortByKey()

joined = joined.map(lambda x : x[0] + ',' + format(x[1][0], '.2f') + ',' + format(x[1][1], '.2f') )

joined.saveAsTextFile('task2c.out')
sc.stop()
