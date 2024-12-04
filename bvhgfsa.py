from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import *
import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\user\AppData\Local\Programs\Python\Python310\python.exe"

schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("FIRSTNAME", StringType(), True),
    StructField("LASTNAME", StringType(), True),
    StructField("URL", StringType(), True),
    StructField("CAMPAIGNS", ArrayType(StringType()), True)
])

data = [
    [1, "nikhil", "rock", "https://tinyurl.8687", ["facebook", "instagram"]],
    [2, "pavan", "kalyan", "https://tinyurl.8681", ["twitter", "fling"]],
    [3, "sai", "arjun", "https://tinyurl.8682", ["whatsapp", "snapchat"]]
]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("sam").getOrCreate()
    df = spark.createDataFrame(data, schema=schema)
    df.show()
    df.printSchema()
    df.select("LASTNAME").show()
    df1= df.withColumn("Id",df.Id+12).show()
    df2= df.withColumn("full_name",concat(col("FIRSTNAME"),lit("_"),col("LASTNAME")))
    df2.show()
    df.show()

