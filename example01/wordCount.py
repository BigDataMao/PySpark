# coding=utf-8
from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('wordCountHelloWorld')
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 需求:wordCount单词计数
    # 读取文件
    file_rdd = sc.textFile('../data/input/words.txt')

    # 将单词进行切割,得到一个存储单词的集合对象
    words_rdd = file_rdd.flatMap(lambda x: x.split(' '))

    # 将单词转换为元祖对象,key是单词,value是数字1
    words_with_1_rdd = words_rdd.map(lambda x: (x, 1))

    # 将元祖的value按照key来分组,对所有的value执行聚合操作(相加)
    result_rdd = words_with_1_rdd.reduceByKey(lambda a, b: a + b)

    # 通过collect方法收集RDD的数据打印输出结果
    collect01_list = result_rdd.collect()
    print(collect01_list)




