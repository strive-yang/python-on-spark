# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 09_dataframe_process_dsl
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 15:38
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

    # column对象的获取
    id_column = df['id']
    subject_column = df['subject']
    # print(id_column,type(id_column))
    # res:  Column<'id'> <class 'pyspark.sql.column.Column'>

    # DSL风格展示
    df.select(['id', 'subject']).show()
    df.select('id', 'subject').show()
    df.select(id_column, subject_column).show()

    # filter API
    df.filter("score<99").show()
    df.filter(df['score'] < 99).show()

    # where API
    df.where('score<99').show()
    df.where(df['score'] < 99).show()

    # group by API，groupBy的返回值为GroupData，不是DataFrame
    # 是一个有分组关系的数据结构，有一些api可以对该数据进行聚合
    # SQL：group by 后接上聚合：sum avg count min max
    # groupBy类似于sql，也有以上五种聚合。调用聚合后才返回DataFrame
    # groupBy只是一个中转对象，最重要处理为DataFrame才行
    df.groupBy('subject').count().show()
    df.groupBy(df['subject']).count().show()

