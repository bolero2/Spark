from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# data_path = "file:///home/bolero/work/spark/mydata/ml-100k/u.data"
data_path1 = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\test1.csv"


# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir",
                                    "C:\\spark-3.0.1-bin-hadoop2.7\\tmp\\hive").appName("test").getOrCreate()

df = spark.read.option("header", "true").csv(data_path1)

dataset = spark.createDataFrame(df.collect()).cache()
dataset.createOrReplaceTempView("test")

df = spark.sql("SELECT col2 FROM test")
df = df.collect()

for r in df:
    print(r)

print("END.")
spark.stop()
exit(0)
