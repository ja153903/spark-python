from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customer-totals")
sc = SparkContext(conf=conf)

orders = sc.textFile("./customer-orders.csv")

def customSplit(line):
    line = line.split(',')
    return (int(line[0]), float(line[2]))

totalSpent = orders.map(customSplit).reduceByKey(lambda x, y: x+y)
totalSpentByLargest = totalSpent.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
results = totalSpentByLargest.collect()

for result in results:
    print("{0} spent {1:.2f}".format(result[1], result[0]))

