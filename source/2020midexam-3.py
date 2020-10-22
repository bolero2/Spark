from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# data_path = "file:///home/bolero/work/spark/mydata/ml-100k/u.data"
data_path1 = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\survey.csv"

spark = SparkSession.builder.config("spark.sql.warehouse.dir",
                                    "C:\\spark-3.0.1-bin-hadoop2.7\\tmp\\hive").appName("survey").getOrCreate()

df = spark.read.option("header", "true").csv(data_path1)
# option("header", "false") -> col name을 survey._c10 등으로 설정하여 사용 가능
# option("header", "true") -> col name을 기존의 col name(country, salary_midpoint...) 등으로 설정하여 사용 가능

dataset = spark.createDataFrame(df.collect()).cache()
dataset.createOrReplaceTempView("survey")

df1 = spark.sql("SELECT COUNT(*) FROM survey WHERE country=='Canada'")
for r in df1.collect():
    print(f"국적이 Canada인 응답자 수: {r[0]}")

total = 0
df2 = spark.sql("SELECT COUNT(*) FROM survey")
for r in df2.collect():
    print(f"전체 응답자 수 : {r[0]}")
    total = r[0]

df3 = spark.sql("SELECT COUNT(*) FROM survey WHERE concat('',salary_midpoint * 1) = salary_midpoint")
for r in df3.collect():
    print(f"salary_midpoint 항목이 무응답인 응답자 수 : {total - r[0]}")

print("END.")
spark.stop()
exit(0)
