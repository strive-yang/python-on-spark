# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 00_spark_session_create
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 10:56
@Email  : yangzy927@qq.com
@Desc   ：简单介绍SparkSession对象，该对象包含了SparkSQL和SparkCore模块
可以通过调用函数获得sc对象
=================================================="""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()

    # 通过SparkSession对象获取SparkContext对象
    sc = spark.sparkContext

    # SparkSQL的hello world
    df = spark.read.csv('../data/input/stu_score.txt', sep=',', header=False)
    df2 = df.toDF('id', 'name', 'score')  # 设置表头
    df2.printSchema()  # 打印表的结构
    df2.show()

    df2.createTempView('score') # 创建临时视图

    # sql语句
    spark.sql("SELECT * FROM score WHERE name='语文' LIMIT 5").show()

    # DSL格式，领域特定语言，指Spark SQL所支持的特定语法和操作，这些语法和操作是专门为处理数据而设计的。
    df2.where("name='语文'").limit(5).show()
