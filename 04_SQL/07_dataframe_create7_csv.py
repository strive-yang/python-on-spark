# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 07_dataframe_create7_csv
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 15:17
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

    # 读取csv文件, 通过string的形式来指定structType
    df = spark.read.format('csv'). \
        option('sep', ';'). \
        option('header', True). \
        option('encoding', 'utf-8'). \
        schema("name STRING,age INT,job STRING"). \
        load('../data/input/sql/people.csv')

    df.printSchema()
    df.show()