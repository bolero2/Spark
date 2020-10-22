from pyspark import SparkConf, SparkContext

# data_path = "file:///home/bolero/work/spark/mydata/ml-100k/u.data"
data_path = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\ml-100k\\u.data"


def loadMovieNames():
    movieNames = {}
    with open("C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\ml-100k\\u-uni.item") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile(data_path)
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda xy: (xy[1], xy[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie: (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()
# max = max(results[0])
for result in results:
    # if result[0] == max:
    #     print(result)
    print(result)
