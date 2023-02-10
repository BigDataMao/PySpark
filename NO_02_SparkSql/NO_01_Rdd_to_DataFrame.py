# coding=utf8
from pyspark import sql

if __name__ == '__main__':
    spark = sql.SparkSession.builder.appName('rddToDataframe').master('local[*]').getOrCreate()

    # TODO spark.sparkContext末尾不能带括号
    # 原因是sc是给spark这个对象自带的一个sparkContext属性取别名
    sc = spark.sparkContext

    # 用sc造一个rdd对象
    rdd = sc.textFile('../data/input/people.txt', minPartitions=None, use_unicode=True). \
        map(lambda x: x.split(','))

    # TODO 方法一:spark.createDataFrame()
    df1 = spark.createDataFrame(rdd, schema=['name', 'age'])
    df1.show(5)

    # TODO 方法二:toDF()
    df2 = rdd.toDF(schema=['name', 'age'])
    df2.show(5)