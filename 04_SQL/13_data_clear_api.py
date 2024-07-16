# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 13_data_clear_api
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-12 15:22
@Email  : yangzy927@qq.com
@Desc   ：异常数据处理，可以对缺失数据、重复数据进行处理
=================================================="""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 1、读取数据
    df=spark.read.format('csv').\
        option('sep',';').\
        option('header',True).\
        load('../data/input/sql/people.csv')

    # 数据清洗：去重
    # dropDuplicates，dataframe的api实现去重，对所有列进行比较去重，保留一条
    # 参数：传入列名，可以按照指定列去重
    df.dropDuplicates().show()
    df.dropDuplicates(['name']).show()

    # 数据清洗：缺失值处理
    # dropna，取出dataframe中的所有列中有缺失值的行删除
    # 参数：thresh，必须满足3个有效列就不删除，否则删除该列
    # 参数：subset，查找指定列的缺失值
    df.dropna().show()
    df.dropna(thresh=3).show()
    df.dropna(thresh=2,subset=['name','age']).show()

    # 数据清洗：缺失值填充
    # DataFrame的fillna对缺失值实现填充
    df.fillna('loss').show()

    # 指定列填充
    df.fillna('N/A',subset=['job']).show()

    # 设定字典对所有的列提供填充规则
    df.fillna({'name':'未知姓名','age':1,'job':'worker'}).show()