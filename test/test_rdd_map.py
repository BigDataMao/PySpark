# coding=utf-8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('wordCountHelloWorld')
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 把一个列表转换为rdd对象
    rdd = sc.parallelize([1, 2, 3, 4])

    # 把这个rdd对象分别测试map函数
    result_map = rdd.map(lambda x: x * 2)
    result_flat_map = rdd.flatMap(lambda x: [x + 1])  # flatMap函数要求映射结果为可迭代对象,比如列表,元组等
    result_flat_map2 = rdd.flatMap(lambda x: [x, x * 2])

    # 查看结果区别,collect()函数就是收集所有元素,组成列表
    print(result_map.collect())
    print(result_flat_map.collect())
    print(result_flat_map2.collect())  # 映射结果扁平化,可迭代对象内部元素等价

    # 新建一个分区的rdd对象,这里分2个区
    rdd2 = sc.parallelize([1, 2, 3, 4], 2)
    result_map_partitions = rdd2.mapPartitions(lambda x: [sum(x)])
    print(result_map_partitions.collect())
