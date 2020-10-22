from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a Spark Session (Note, the config section is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "C:\\spark-3.0.1-bin-hadoop2.7\\tmp\\hive").appName("Spark-SQL").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode('utf8')), age=int(fields[2]), numFriends=int(fields[3]))


lines = spark.sparkContext.textFile("C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\data_1013\\fakefriends.csv")
people = lines.map(mapper)
# print(people) -> PythonRDD[2] at RDD at PythonRDD.scala:53

# Infer the Schema, and register the Dataframe as a table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over Dataframes that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()