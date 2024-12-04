from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import os
import pandas as pd
from pyspark.sql.types import StringType

os.environ["PYSPARK_PYTHON"] = r"C:\Users\user\AppData\Local\Programs\Python\Python310\python.exe"

svar = SparkSession.builder.appName("app234").getOrCreate()

schema = StructType([
    StructField("name",StringType(),False),
    StructField("age", IntegerType(),False),
    StructField("DOB",StringType(),True)
])

list = [("Ram",24, "20-11-2000"),("Sham",22, "20-11-2002"),("Lucky",20, "20-11-2004"),("Neem",26, "20-11-1998")]

sdfvar = svar.createDataFrame(list,schema)

pvar1 = sdfvar.toPandas()

"""
print("first 2 rows of df data")
print(pvar1.head(n=2))
print("info of df")
print(pvar1.info())
print("shape of df")
print(pvar1.shape)
"""

pvar2 = pvar1.replace("Ram", "Ramu")
print(pvar2.head(n=2))

print(pvar2.sort_values(by='age', ascending=True))

