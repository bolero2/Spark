from pyspark import SparkConf, SparkContext
import collections

# data_path = "file:///home/bolero/work/spark/mydata/ml-100k/u.data"
data_path = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\ml-100k\\u.data"

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile(data_path)
# print(f"lines={lines}")

ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
# print(f"result={result}")

sortedResults = collections.OrderedDict(sorted(result.items()))

print("\n===== Result =====")

for key, value in sortedResults.items():
	# print(f"key={key}	value={value}")
	print("%s	%i" % (key, value))
	
print("===== Finish =====\n")
