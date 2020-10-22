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
movies = lines.map(lambda x: Row(movieID=int(x.split()[1])))
# print(movies) -> PythonRDD[2] at RDD at PythonRDD.scala:53

# Convert that to a DataFrame
movieDataset = spark.createDataFrame(movies).cache()
movieDataset.createOrReplaceTempView("movies")

# 가장 많은 평가를 받은 영화 순위
row300 = spark.sql("SELECT movieID, COUNT(movieID) AS count FROM movies GROUP BY movieID ORDER BY count DESC LIMIT 10")

print("+-------+-----+")
print("|movieID|count|")
print("+-------+-----+")
for r1 in row300.collect():
    # row_count += 1
    print("|", end="")
    print("%7d" % r1[0], end="")
    print("|", end="")
    print("%5d" % r1[1], end="")
    print("|", end="\n")
print("+-------+-----+")
# print(f"row count={row_count}")
print("END.")
spark.stop()
exit(0)
#
# # Some SQL-style magic to sort all movies by popularity in one line!
# topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()
#
# # Show the results at this point:
# topMovieIDs.show()
# """
# +-------+-----+
# |movieID|count|
# +-------+-----+
# |     50|  583|
# |    258|  509|
# |    100|  508|
# |    181|  507|
# |    294|  485|
# |    286|  481|
# |    288|  478|
# |      1|  452|
# |    300|  431|
# |    121|  429|
# |    174|  420|
# |    127|  413|
# |     56|  394|
# |      7|  392|
# |     98|  390|
# |    237|  384|
# |    117|  378|
# |    172|  367|
# |    222|  365|
# |    313|  350|
# +-------+-----+
# """
#
# # Grab the top 10
# top10 = topMovieIDs.take(10)
#
# # Print the results
# print("\n")
# for result in top10:
#     # Each row has movieID, count as above.
#     print("%s: %d" % (nameDict[result[0]], result[1]))
#
# # Stop the session
# spark.stop()
