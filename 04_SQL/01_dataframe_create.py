# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 01_dataframe_create
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-11 11:29
@Email  : yangzy927@qq.com
@Desc   ：DataFrame构建方式1
描述了如何将rdd转换为dataFrame，以及使用sql查询语句
=================================================="""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc=spark.sparkContext

    # 基于RDD转换为DataFrame
    file_rdd=sc.textFile('../data/input/sql/people.txt').\
        map(lambda x:x.split(',')).\
        map(lambda x:(x[0],int(x[1])))

    # 构建dataframe对象 -- rdd转为df
    # 参数1 转换的rdd
    # 参数2 列名，依次提供字符串名
    df=spark.createDataFrame(file_rdd,schema=['name','age'])

    # 打印DataFrame表结构信息
    df.printSchema()

    # 打印df中的数据
    # 参数1 表示显示信息个数，默认20
    # 参数2 表示是否对列进行截断，如果列中数据的长度超过20字符，后面的内容不显示
    # False表示不截断全部显示，默认True
    df.show(20,False)

    # 不显示过长列名的部分
    df.show(20,True)

    # 将DF对象转换为临时视图表，可以使用sql查询
    df.createOrReplaceTempView('people')
    spark.sql("SELECT * FROM people WHERE age<30").show()



