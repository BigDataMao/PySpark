# coding=utf8
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 通过SparkSession类创建sparkSQL入口
    spark = SparkSession.builder.appName('testSession').master('local[*]').getOrCreate()

    # RDD编程也支持
    sc = spark.sparkContext

    # 由spark对象调用read函数得到dataframe对象
    df1 = spark.read.csv('../data/input/stu_score.csv', encoding='utf8', header=None)

    # 给df1添加表头
    df2 = df1.toDF('id', 'course', 'score')
    df2.printSchema()

    # df转表格
    df2.createOrReplaceTempView('t_score')

    # SQL风格输出
    spark.sql('''
        select * from t_score t where t.course='语文' limit 5
    ''').show()

    # DSL风格
    df2.filter(df2.course=='语文').limit(5).show() # filter,where类似但语法不同