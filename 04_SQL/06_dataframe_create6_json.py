# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 06_dataframe_create6_json
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 15:11
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # JSON类型自带schema对象
    df = spark.read.format('json').load('../data/input/sql/people.json')
    df.printSchema()
    df.show()
