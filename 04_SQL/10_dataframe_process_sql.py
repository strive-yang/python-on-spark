# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 10_dataframe_process_sql
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 16:44
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

    df = spark.read.format('csv'). \
        schema("id INT,subject STRING,score INT"). \
        load('../data/input/sql/stu_score.txt')

    # 注册成临时表
    df.createTempView('score')  # 注册临时视图\表
    df.createOrReplaceTempView('score_2')  # 注册或者替换临时视图
    df.createGlobalTempView('score_3')  # 注册全局临时视图，使用时要在前面带上global_temp

    # 可以通过spark session对象的sql api实现sql语句
    spark.sql("SELECT subject,COUNT(*) AS cnt FROM score GROUP BY subject").show()
    spark.sql("SELECT subject,COUNT(*) AS cnt FROM score_2 GROUP BY subject").show()
    spark.sql("SELECT subject,COUNT(*) AS cnt FROM global_temp.score_3 GROUP BY subject").show()
