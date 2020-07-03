import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# logFile = "/Users/surendranathreddykudumula/Softwares/spark-2.4.5-bin-hadoop2.7/README.md"
spark = SparkSession.builder.appName("SimpleApp").master("local[*]").getOrCreate()
# logData = spark.read.text(logFile).cache()

df = spark.read.json("/Users/surendranathreddykudumula/Softwares/spark-2.4.5-bin-hadoop2.7/examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df.show()
df.filter(df['age'] > 21).show()
df2 = df.filter(df.age > 3)
print("df2")
df2.show()
df3 = df.filter(df.name == "Andy")
print("df3")
df3.show()

spark.stop()
