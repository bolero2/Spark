from pyspark import SparkConf, SparkContext
import re

# data_path = "file:///home/bolero/work/spark/mydata/book.txt"
data_path = "C:\\spark-3.0.1-bin-hadoop2.7\\mydata\\book.txt"

def normalizeWords(text):
	return re.compile(r'\n+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile(data_path)
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

from operator import itemgetter
results.sort(key=itemgetter(0), reverse=True)

for result in results:
	count = str(result[0])
	word = result[1].encode('ascii', 'ignore')
	if (word):
		print(word.decode() + ":\t\t" + count)
