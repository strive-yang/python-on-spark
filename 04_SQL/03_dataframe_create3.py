# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 03_dataframe_create3
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 14:47
@Email  : yangzy927@qq.com
@Desc   ：DataFrame构建方式3
通过toDF实现rdd向DataFrame的转化
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

    # toDF的api创建DataFrame
    df1=file_rdd.toDF(['name','age'])
    df1.printSchema()
    df1.show()

    # toDF的方式2，structType来构建
    schema=StructType().\
        add('name',StringType(),nullable=True).\
        add('age',IntegerType(),nullable=False)

    df2=file_rdd.toDF(schema=schema)
    df2.printSchema()
    df2.show()