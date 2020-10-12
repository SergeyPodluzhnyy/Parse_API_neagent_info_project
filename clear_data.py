import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import unix_timestamp, from_unixtime

spark = SparkSession.builder.appName('neagent_parse').getOrCreate()

filePath = sys.argv[1]+'/result.csv'

df = spark.read.csv(filePath, inferSchema=True, header=False, encoding='cp1251')

split_column0 = F.split(df['_c1'], ",")
split_column3 = F.split(df['_c4'], "кв.м")

df=df.withColumn('_c0',from_unixtime(unix_timestamp('_c0', 'dd.MM.yyyy')).cast('timestamp'))\
      .withColumn('title', split_column0.getItem(0))\
      .withColumn('housing', split_column0.getItem(1))\
      .withColumn('street', split_column0.getItem(3))\
      .withColumn('owner', split_column0.getItem(4))\
      .withColumn('area', F.regexp_replace(split_column3.getItem(0), r'\D', '' ).cast('int'))\
      .withColumn('floor_height', F.regexp_replace(split_column3.getItem(1), r'эт.', '' ))\
      .withColumnRenamed('_c0', 'actual_date')\
      .withColumnRenamed('_c2', 'price')\
      .withColumnRenamed('_c3', 'district')\
      .withColumnRenamed('_c5', 'key')\
      .drop('_c1', '_c4')

df=df.withColumn('price_sqm', (df['price']/df['area']).cast('int'))
df=df.select('key', 'actual_date', 'district', 'title', 'housing', 'street', 'owner', 'floor_height', 'area', 'price', 'price_sqm')

df.write.csv(sys.argv[1]+'/result_01.csv', header = True)