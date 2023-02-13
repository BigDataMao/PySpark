# coding=utf8
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('test_JDBC'). \
        master('local[*]'). \
        getOrCreate()

    df = spark.read. \
        format('jdbc'). \
        option("driver", "org.apache.hive.jdbc.HiveDriver").\
        option('url', 'jdbc:hive2://node2:10000/test'). \
        option('dbtable','part'). \
        option('user', 'root'). \
        option('password', 'mxw19910712'). \
        load()
    df.show()