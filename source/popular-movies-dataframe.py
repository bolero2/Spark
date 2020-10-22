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
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "C:\\spark-3.0.1-bin-hadoop2.7\\tmp\\hive").appName("PopularMovies").getOrCreate()

# Load up our movie ID -> name dictionary
nameDict = loadMovieNames()

# Get the raw data
lines = spark.sparkContext.textFile(data_path1)
# Convert it to a RDD of Row objects
movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
# Convert that to a DataFrame
movieDataset = spark.createDataFrame(movies)

# Some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()

# Show the results at this point:

"""
|movieID|count|
+-------+-----+
|     50|  584|
|    258|  509|
|    100|  508|
"""
topMovieIDs.show()

# Grab the top 10
top10 = topMovieIDs.take(10)

# Print the results
print("\n")
for result in top10:
    # Each row has movieID, count as above.
    print("%s: %d" % (nameDict[result[0]], result[1]))

# Stop the session
spark.stop()
