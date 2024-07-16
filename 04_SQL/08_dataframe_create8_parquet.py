# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 08_dataframe_create8_parquet
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 15:25
@Email  : yangzy927@qq.com
@Desc   ：parquet是spark中常用的一种列式存储文件格式
和Hive中的ORC差不多，都是列存储格式
parquet内置schema，序列化存储在文件中
=================================================="""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format('parquet'). \
        load('../data/input/sql/users.parquet')

    df.printSchema()
    df.show()
