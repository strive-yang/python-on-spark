# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-29 10:01
@Email  : yangzy927@qq.com
@Desc   ：DataFrame join操作
在Join的过程中，左边和右边都不能为None，可以是空数据的表但是需要带Schema，且Schema中有指定的列名。
=================================================="""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

if __name__ == '__main__':
    start_time = time.time()

    # 0、初始化环境
    spark = SparkSession.builder. \
        master('local[*]'). \
        config('spark.executor.memory', '16g'). \
        config('spark.driver.memory', '8g'). \
        appName('DataFrame join'). \
        getOrCreate()
    sc = spark.sparkContext

    # 1、创建两个DataFrame
    heroes_data = [('Deadpool', 3), ('Iron man', 1), ('Groot', 7)]
    race_data = [('Kryptonian', 5), ('Mutant', 3), ('Human', 1)]
    heroes = spark.createDataFrame(heroes_data, ['name', 'id'])
    races = spark.createDataFrame(race_data, ['reace_name', 'id'])

    # 2、对表按照id列进行左外连接 left\leftouter
    heroes.join(races, on='id', how='left').show()  # 左外连接，按照id连接

    # 3、计算两表的笛卡尔积
    heroes.crossJoin(races).show()

    # 4、按照id对两表内连接
    heroes.join(races, on='id', how='inner').show()

    # 5、按照id对两表全外连接
    heroes.join(races, on='id', how='full').show()

    # 6、左半连接 leftsemi
    heroes.join(races, on='id', how='leftsemi').show()

    # 7、left anti，左半连接取反
    heroes.join(races, on='id', how='leftanti').show()


    print("运行时间：", round(time.time() - start_time, 4))
