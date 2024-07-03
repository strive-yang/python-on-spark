# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 14_RDD_operators_intersection
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:22
@Email  : yangzy927@qq.com
@Desc   ：intersection 求两个rdd的交集
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6])
    rdd2 = sc.parallelize([4, 5, 6, 7, 8, 9])

    print(rdd1.intersection(rdd2).collect())
