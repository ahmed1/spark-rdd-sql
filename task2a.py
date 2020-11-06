from pyspark import SparkContext
from csv import reader
import sys
from operator import add


sc = SparkContext()

joined = sc.textFile(sys.argv[1], 1) # "/user/as9621/task1a.out")
joined = joined.mapPartitions(lambda x : reader(x))
joined = joined.map(lambda line : [elem.strip() for elem in line])

# fare_amount x[15]

# [0, 5] -- 273109
range_0_5 = joined.filter(lambda x : (float(x[15]) <= 5)).filter(lambda x : (float(x[15]) >= 0)).count()

# (5, 15]
range_5_15 = joined.filter(lambda x : (float(x[15]) <= 15)).filter(lambda x : (float(x[15]) > 5)).count()

# (15, 30]
range_15_30 = joined.filter(lambda x : (float(x[15]) <= 30)).filter(lambda x : (float(x[15]) > 15)).count()

# (30, 50]
range_30_50 = joined.filter(lambda x : (float(x[15]) <= 50)).filter(lambda x : (float(x[15]) > 30)).count()


# (50, 100]
range_50_100 = joined.filter(lambda x : (float(x[15]) <= 100)).filter(lambda x : (float(x[15]) > 50)).count()


# [>100)
range_100_plus = joined.filter(lambda x : (float(x[15]) > 100)).count()


res = sc.parallelize( [ ('0,5,' + str(range_0_5)), ('5,15,' + str(range_5_15)), ('15,30,' + str(range_15_30)), ('30,50,' +str(range_30_50)), ('50,100,'+  str(range_50_100))  , ('>100,' +  str(range_100_plus)) ]  )


res.saveAsTextFile('task2a.out')

sc.stop()
