from pyspark import SparkConf, SparkContext

# data_path = "file:///home/bolero/work/spark/mydata/ml-100k/u.data"
data_path1 = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\data_1013\\Marvel-Names.txt"
data_path2 = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\data_1013\\Marvel-Graph.txt"

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf=conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode('utf8'))


names = sc.textFile(data_path1)
namesRdd = names.map(parseNames)

lines = sc.textFile(data_path2)
pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y: x + y)
flipped = totalFriendsByCharacter.map(lambda xy: (xy[1], xy[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " +
      str(mostPopular[0]) + " co-appearances.")