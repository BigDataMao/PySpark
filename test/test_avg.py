from pyspark import SparkContext

# 创建SparkContext
sc = SparkContext("local[1]", "reduceByKey example")

# 示例数据
rdd = sc.parallelize([(1, 2), (1, 4), (2, 6), (2, 8), (3, 10), (3, 12), (4, 14), (4, 16)])


# 定义累加器：(sum, count)
def seq_op(acc, value):
    return acc[0] + value, acc[1] + 1


# 定义分区合并器：(sum1, count1), (sum2, count2)
def comb_op(acc1, acc2):
    return acc1[0] + acc2[0], acc1[1] + acc2[1]


# 累加数据
average = rdd.aggregateByKey((0, 0), seq_op, comb_op)

# 计算平均值
result = average.mapValues(lambda x: x[0] / x[1])
print("平均值：", result.collect())
