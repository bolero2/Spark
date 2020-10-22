from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# data_path = "file:///home/bolero/work/spark/mydata/ml-100k/u.data"
data_path1 = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\ml-100k\\u.data"
data_path2 = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\ml-100k\\u-uni.item"


def loadMovieNames():
    movieNames = {}
    with open(data_path2) as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir",
                                    "C:\\spark-3.0.1-bin-hadoop2.7\\tmp\\hive").appName("PopularMovies").getOrCreate()

# Load up our movie ID -> name dictionary
nameDict = loadMovieNames()

# Get the raw data
lines = spark.sparkContext.textFile(data_path1)
# Convert it to a RDD of Row objects
movies = lines.map(lambda x: Row(movieID=int(x.split()[1]), rating=int(x.split()[2])))
# print(movies) -> PythonRDD[2] at RDD at PythonRDD.scala:53

# Convert that to a DataFrame
movieDataset = spark.createDataFrame(movies).cache()
movieDataset.createOrReplaceTempView("movies")

# 가장 많은 평가를 받은 영화 순위
row300 = spark.sql("SELECT DISTINCT movieID, rating FROM movies WHERE rating==1 ORDER BY rating LIMIT 10")

for r in row300.collect():
    print(f"{nameDict[r[0]]} =>> {float(r[1])}")

print("END.")
spark.stop()
exit(0)
