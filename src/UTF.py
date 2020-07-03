from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").master("local[*]").getOrCreate()

file = "/Users/surendranathreddykudumula/PycharmProjects/PySparkTemplate/output/out/part-00000-c49fdc12-a9ff-45c2-85c5-f0ddb45f8624-c000.csv"
df = spark.read.option("encoding", "UTF-8").option("delimeter","|").csv(file)

# df1 = spark.read.option("encoding", "UTF-8").csv(file)

print("UTF")
df.show(truncate=False)

