from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

import os
print(os.listdir(os.getcwd()))

spark = SparkSession.builder.appName("SimpleApp").master("local[*]")\
    .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf()).enableHiveSupport()\
    .getOrCreate()

file = "/resources/iso.csv"

df = spark.read.option("encoding", "ISO-8859-1").option("delimiter","|").csv(file)
# df = spark.read.option("encoding", "ISO-8859-1").csv(file, sep="|")
df1 = spark.read.option("encoding", "UTF-8").option("delimiter","|").csv(file)

print(os.listdir(os.getcwd()))

print("ISO")
df.show(truncate=False)

print("UTF")
df1.show(truncate=False)

df.write.option("encoding","UTF-8").mode("overwrite").saveAsTable("utf_table")
# df.write.option("encoding","UTF-8").mode("overwrite")\
#     .csv("/Users/surendranathreddykudumula/PycharmProjects/PySparkTemplate/output/out")
# SparkContext.setSystemProperty("hive.metastore.uris", "thrift://localhost:9083")
# Read from Hive
df_load = spark.sql("SELECT * FROM utf_table")
df_load.show()
