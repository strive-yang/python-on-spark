# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 04_dataframe_create4
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 14:55
@Email  : yangzy927@qq.com
@Desc   ：DataFrame构建方式4
Pandas.DataFrame 转换为SparkSql的DataFrame
=================================================="""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 基于Pandas的DataFrame对象构建SparkSQL的DataFrame对象
    pdf=pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ['张三丰', '张蓉', '至尊宝', '胡汉三', '郭靖'],
        'age': [11, 21, 22, 13, 45]
    })
    df=spark.createDataFrame(pdf)
    df.printSchema()
    df.show()
