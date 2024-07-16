# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 05_dataframe_create5_text
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 15:05
@Email  : yangzy927@qq.com
@Desc   ：
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

    # 1、构建structType，text数据源，读取数据的特点是，将一整行只作’一列’读取，默认列名是valuem，类型string
    schema = StructType().add('data', StringType(), nullable=True)
    df=spark.read.format('text'). \
        schema(schema=schema).\
        load('../data/input/sql/people.txt')

    df.printSchema()
    df.show()

