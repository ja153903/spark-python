from pyspark import SparkConf, SparkContext
import re 

conf = SparkConf().setMaster("local").setAppName("MapFlatMap")
sc = SparkContext(conf=conf)

lines = sc.textFile("./Book.txt")

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# now we want to flatmap the values and then apply the sum to it
words = lines.flatMap(normalizeWords)
wordCount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountSorted = wordCount.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountSorted.collect()

for result in results:
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(f"{word}: {result[0]}")

# print(f"This book has {numWords} words")


