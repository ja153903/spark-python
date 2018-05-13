from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

conf = SparkConf().setMaster("local").setAppName('FriendsByAge')
sc = SparkContext(conf=conf)

lines = sc.textFile("./fakefriends.csv")
rdd = lines.map(parseLine)

#rdd.foreach(print)

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# totalsByAge.foreach(print)

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

averagesByAge.foreach(print)
