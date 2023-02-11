# coding=utf8
from pyspark import sql
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = sql.SparkSession.builder.appName('rddToDataframe').master('local[*]').getOrCreate()

    # TODO spark.sparkContext末尾不能带括号
    # 原因是sc是给spark这个对象自带的一个sparkContext属性取别名
    sc = spark.sparkContext

    # 用sc造一个rdd对象
    rdd = sc.textFile('../data/input/people.txt', minPartitions=None, use_unicode=True). \
        map(lambda x: x.split(',')). \
        map(lambda x: [x[0], int(x[1])])  # 强转格式

    # TODO 方法一:spark.createDataFrame(rdd,schema)
    df1 = spark.createDataFrame(rdd, schema=['name', 'age'])
    df1.printSchema()
    # show()方法,参数1:展示几行,默认20,参数2:截断几个字符串
    df1.where(df1.age > 20).show(1, 2)

    # TODO 方法二:先构建StructType对象作为schema,信息更完整
    schema = StructType(). \
        add('name', StringType(), nullable=True). \
        add('age', IntegerType(), nullable=False)
    df2 = spark.createDataFrame(rdd, schema=schema)
    df2.show(2, truncate=False)

    # TODO 方法三:toDF()快捷转换
    df3 = rdd.toDF(schema=schema)
    df3.show(20)