from pyspark import SparkConf, SparkContext

# data_path = "file:///home/bolero/work/spark/mydata/fakefriends.csv"
data_path = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\data_1013\\fakefriends.csv"

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

def parseLine(line):
	fields = line.split(',')
	age = int(fields[2])
	numFriends = int(fields[3])
	return (age, numFriends)


lines = sc.textFile(data_path)
# print(f"lines={lines}")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# Default=random / sortByKey = Key Sorting
# result_sort = averagesByAge.sortByKey()

results = averagesByAge.collect()
# results -> 'list' type data and type of element is 'tuple'

# sort By Value -> Using 'sorted' function
# -> dict_data = dict(data)
# -> sdict = sorted(dict_data.items(), key=lambda item: item[1])
# After, convert the data type to tuple. (result = tuple(result))
results = dict(results)
result_sort = sorted(results.items(), key=lambda item: item[1])

print("\n===== Result =====")

for result in result_sort:
	result = tuple(result)
	print(result)
	
print("===== Finish =====\n")
