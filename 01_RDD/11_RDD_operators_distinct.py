# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 11_RDD_operators_distinct
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:07
@Email  : yangzy927@qq.com
@Desc   ：distinct对重复数据的去重
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 1, 2, 3, 4, 5, 1, 1, 2, 3])

    # distinct实现对重复数据的去重
    print(rdd.distinct().collect())

    rdd2=sc.parallelize([('a',1),('a',1),('b',1)])
    print(rdd2.distinct().collect())