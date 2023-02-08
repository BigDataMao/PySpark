from pyspark.sql import SparkSession
from pyspark import SparkContext

# 创建SparkContext
sc = SparkContext("local[1]", "sparkSQL avg example")

# 创建SparkSession
spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()

# 创建RDD键值对数据
rdd = sc.parallelize([(1, 2), (3, 4), (3, 6), (3, 8), (5, 10), (5, 12), (7, 14), (7, 16)])

# 将RDD键值对数据转换为DataFrame
df = rdd.toDF(["key", "value"])

# 注册DataFrame为临时表
df.createOrReplaceTempView("temp_table")

# 使用Spark SQL API编写SQL语句
result = spark.sql("SELECT key, AVG(value) AS avg_value FROM temp_table GROUP BY key ORDER BY key")

# 显示结果
result.show()

# 关闭SparkSession
spark.stop()
