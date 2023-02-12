# coding=utf8
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('test_file_to_DataFrame'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 读取TEXT文件
    # 注意结果每行只有一个字段
    people_text = spark.read.format('text'). \
        schema('line string'). \
        load('../../data/input/people.txt')
    people_text.show()
    # TODO 后面再来做拆分

    # 读取JSON数据
    people_json = spark.read.format('json'). \
        load('../../data/input/people.json')
    people_json.show()

    # 读取CSV数据
    people_csvDF = spark.read. \
        format('csv'). \
        option('sep', ';'). \
        option('header', True). \
        option('encoding', 'utf8'). \
        option('inferSchema', True). \
        load('../../data/input/people.csv')
    people_csvDF.printSchema()
    people_csvDF.show(10)

    # 读取parquet列式存储文件,单词本意是镶木地板
    # parquet文件自带:列明,格式,且序列化压缩
    users_parquetDF = spark.read. \
        format('parquet'). \
        load('../../data/input/users.parquet')
    users_parquetDF.printSchema()
    users_parquetDF.show()

    # TODO 读取其他