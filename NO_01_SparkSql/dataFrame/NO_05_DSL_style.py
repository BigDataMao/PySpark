# coding=utf8
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('test_DSL_style'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read. \
        format('csv'). \
        option('sep', ','). \
        option('header', False). \
        schema('ID int,course string,score int'). \
        load('../../data/input/stu_score.txt')

    # select:可变参数或数组
    df.select('ID', 'course').show(3)
    df.select(['ID', 'course']).show(3)

    # where/filter
    df.filter("ID<4").show()
    df.filter(df.ID < 4).show()
    df.where(df['ID'] < 4).show()

    # groupby
    gb = df.groupby('course')
    # groupby之后的不是DataFrame,是GroupedData类
    print(type(gb))
    # 聚合函数以后才是DataFrame
    gb.count().show()
    gb.avg('score').show()
    gb.mean('score').show()
    gb.min('ID').show()
    gb.agg({'ID': 'max', 'score': 'avg'}).show()

    from pyspark.sql import functions as F

    gb.agg(F.avg('score').alias('平均分'), F.max('score').alias('最高分')).show()

    # 演示连表
    scoreDF = spark.createDataFrame(
        [('张三', 100, 1), ('李四', 80, 2), ('王五', 55, 3)],
        ['name', 'score', 'class']
    )

    classDF = spark.createDataFrame(
        [(1, '火箭班'), (2, '快班'), (3, '平行班')],
        ['class', 'cName']
    )

    scoreDF.join(classDF, scoreDF['class'] == classDF['class']). \
        select(scoreDF['name'].alias('名字'), classDF['cName'].alias('班型')). \
        show()
