from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext.builder().master('local[*]').appName("something").getOrCreate()

# Create a range of numbers
big_list = range(1000)
rdd = sc.parallelize(big_list, 2)

# Filter for odd numbers
odds = rdd.filter(lambda x: x % 2 != 0)

# Take the first 5 odd numbers and print them
print(odds.take(5))  # Output: [1, 3, 5, 7, 9]

# Stop the SparkContext
sc.stop()
