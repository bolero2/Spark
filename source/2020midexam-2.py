from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

data_path1 = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\ml-100k\\u.data"
data_path2 = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\ml-100k\\u-uni.item"


def loadMovieNames():
    movieNames = {}
    with open(data_path2) as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.config("spark.sql.warehouse.dir",
                                    "C:\\spark-3.0.1-bin-hadoop2.7\\tmp\\hive").appName("PopularMovies").getOrCreate()
nameDict = loadMovieNames()

lines = spark.sparkContext.textFile(data_path1)
movies = lines.map(lambda x: Row(movieID=int(x.split()[1]), rating=int(x.split()[2])))

movieDataset = spark.createDataFrame(movies).cache()
movieDataset.createOrReplaceTempView("movies")

df1 = spark.sql("SELECT movieID, SUM(rating) AS sum_rating, COUNT(movieID) AS count FROM movies GROUP BY movieID ORDER BY count DESC")

movieDataset = spark.createDataFrame(df1.collect()).cache()
movieDataset.createOrReplaceTempView("movies")

df1 = spark.sql("SELECT movieID, sum_rating/count AS avg_rating FROM movies WHERE count >= 100 ORDER BY avg_rating DESC LIMIT 10")

for r in df1.collect():
    print(f"{nameDict[r[0]]} =>> {r[1]}")

print("END.")
spark.stop()
exit(0)


