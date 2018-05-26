from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

lines = sc.textFile("./ml-100k/u.data")
# (movie_id, count)
movies = lines.map(lambda x: (int(x.split()[1]), 1))
# reduceByKey allows us to add up all the counts of similar keys
movieCounts = movies.reduceByKey(lambda x, y: x + y)

# flip the tuple
flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)
