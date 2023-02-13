# coding=utf8
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('test_SQL_style'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    words = spark.read. \
        format('text'). \
        load('../../data/input/words.txt')

    words.createOrReplaceTempView('t_words')

    result_SQL = spark.sql('''
        select word ,count(1) count  from (
            select explode(split(value,' ')) word from t_words
        ) t group by word order by count desc
    ''')

    result_SQL. \
        write. \
        mode('overwrite'). \
        format('csv'). \
        option('header', True). \
        option('sep', ','). \
        save('../../data/output/wordCountResult')
