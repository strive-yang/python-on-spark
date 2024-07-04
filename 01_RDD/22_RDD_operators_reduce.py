# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 22_RDD_operators_reduce
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 17:38
@Email  : yangzy927@qq.com
@Desc   ：reduce，对rdd数据按照传入数据进行聚合
=================================================="""
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd=sc.parallelize(range(1,11))
    print(rdd.reduce(lambda a, b: a + b))