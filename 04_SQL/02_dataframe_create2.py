# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 02_dataframe_create2
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 11:49
@Email  : yangzy927@qq.com
@Desc   ：DataFrame构建方式2
构建表结构的描述对象：structType对象，基于structType实现rdd to df的转换
=================================================="""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 基于RDD转换为DataFrame
    file_rdd = sc.textFile('../data/input/sql/people.txt'). \
        map(lambda x: x.split(',')). \
        map(lambda x: (x[0], int(x[1])))

    # 构建表结构的描述对象：structType对象
    schema = StructType().\
        add('name', StringType(), nullable=True). \
        add('age', IntegerType(), nullable=False)

    # 基于structType构建rdd2df的转换
    df = spark.createDataFrame(file_rdd, schema=schema)

    df.printSchema()
    df.show()
