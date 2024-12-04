from pyspark import *
from pyspark.sql import *

#spark session create
svar = SparkSession.builder.appName("app1").getOrCreate()
var1 = [1,2,3,4,5]
#create spartcontext for rdd
svar2 = svar.sparkContext.parallelize(var1).collect()
print(svar2)


