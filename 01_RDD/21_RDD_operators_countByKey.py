# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 21_RDD_operators_countByKey
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 17:28
@Email  : yangzy927@qq.com
@Desc   ：countByKey，action算子，统计key出现的次数，返回值是字典
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.textFile('../data/input/words.txt')
    rdd2 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))

    # 通过countByKey对key进行计数
    print(rdd2.countByKey())
    print(type(rdd2.countByKey()))
