from pyspark.sql import SparkSession
import pandas as pd

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('test_pandas'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 构建pandas的DataFrame对象
    pandasDF = pd.DataFrame(
        {
            'ID': [1, 2, 3],
            'name': ['紫川星', '斯特林', '帝林'],
            'age': [18, 19, 20]
        }
    )

    # 转spark的DF
    df1 = spark.createDataFrame(pandasDF)
    df1.printSchema()
    df1.show()
