from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MapFlatMap")
sc = SparkContext(conf=conf)

lines = sc.textFile("redfox.txt")
upperEverything = lines.map(lambda x: x.upper())

results = upperEverything.collect()

print(results)


# flatmap() can create many new elements from each one
# this splits into every single word and makes it uppercase
words = lines.flatMap(lambda x: x.upper())

other = words.collect()

print(other)