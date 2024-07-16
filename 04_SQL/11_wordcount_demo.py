# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 11_wordcount_demo
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 16:57
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # TODO 1: SQL风格处理
    rdd = sc.textFile('../data/input/words.txt'). \
        flatMap(lambda x: x.split(' ')). \
        map(lambda x: [x])

    df = rdd.toDF(['word'])

    # 创建临时表
    df.createTempView('words')

    spark.sql('SELECT word,COUNT(*) AS cnt FROM words GROUP BY word order by cnt DESC').show()

    # TODO 2: DSL风格处理
    df2 = spark.read.format('text'). \
        load('../data/input/words.txt')

    # withColumn方法
    # 对已存在的列进行操作，返回一个新的列，如果名字和老列相同，替换，否则作为新列存在。
    df2=df2.withColumn('value',F.explode(F.split(df2['value'],' ')))
    df2.groupBy('value').\
        count().\
        withColumnRenamed('value','word').\
        withColumnRenamed('count','cnt').\
        orderBy('cnt',ascending=False).\
        show()