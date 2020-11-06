from pyspark import SparkContext
import sys
from csv import reader

sc = SparkContext()


trip_lines = sc.textFile(sys.argv[1], 1)
trip_lines = trip_lines.mapPartitions(lambda x : reader(x))
fare_lines = sc.textFile(sys.argv[2], 1)
fare_lines = fare_lines.mapPartitions(lambda x : reader(x))

trip_lines = trip_lines.filter(lambda x : "medallion" not in x)
fare_lines = fare_lines.filter(lambda x : "medallion" not in x)

trip_lines = trip_lines.map(lambda r : (  (r[0],r[1],r[2],r[5]), (r[3],r[4],r[6],r[7],r[8],r[9],r[10], r[11],r[12],r[13])    ))

fare_lines = fare_lines.map(lambda r : (  (r[0],r[1],r[2],r[3]), (r[4],r[5],r[6],r[7],r[8],r[9],r[10])     ))

joined = trip_lines.join(fare_lines)

joined = joined.sortBy(lambda r : (r[0], r[0][1], r[0][3]))


joined = joined.map(lambda r : ','.join(key for key in r[0]) + ',' + ','.join(val_left for val_left in r[1][0]) + ',' + ','.join(val_right for val_right in r[1][1]))


joined.saveAsTextFile("task1a.out")




sc.stop()
