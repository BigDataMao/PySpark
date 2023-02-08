from pyspark import SparkContext

# 创建SparkContext
sc = SparkContext("local[1]", "reduceByKey example")

# 创建RDD
rdd = sc.parallelize([(1, 2), (3, 4), (3, 6), (3, 8)])

# 使用reduceByKey函数计算每个键对应的总和
result1 = rdd.reduceByKey(lambda a, b: a + b)

# 输出结果
print(result1.collect())

# 按键分组
result2 = rdd.groupByKey().mapValues(list)

print(result2.collect())

