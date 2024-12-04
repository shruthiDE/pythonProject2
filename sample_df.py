from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import os
import pandas as pd
from pyspark.sql.types import StringType

os.environ["PYSPARK_PYTHON"] = r"C:\Users\user\AppData\Local\Programs\Python\Python310\python.exe"

## building spark session
svar = SparkSession.builder.appName("app234").getOrCreate()

schema = StructType([
    StructField("name",StringType(),False),
    StructField("age", IntegerType(),False),
    StructField("DOB",StringType(),True)
])

list = [("Ram",24, "20-11-2000"),("Sham",22, "20-11-2002")]

sdfvar = svar.createDataFrame(list,schema)

####

schema2 = StructType([
    StructField("name",StringType(),False),
    StructField("Qualification",StringType(),False),
    StructField("Collage",StringType(),False),
    StructField("currentOrg",StringType(),False)
])

list2 =[("Ram","B.Tech","CBIT","Capgemini"),("James","BSc","KU","IBM"),("Roman","MBA","JNTU","TCS")]
sdfvar2 = svar.createDataFrame(list2,schema2)

sdfvar.show()
var1 = sdfvar[["name"]]
var1.show()
var2 = sdfvar[sdfvar["name"]=="Ram"]
var2.show()

pdvar = sdfvar.toPandas()
pdvar2 = sdfvar2.toPandas()

var3 = pd.merge(pdvar,pdvar2,on='name', how='outer')

print(var3)
