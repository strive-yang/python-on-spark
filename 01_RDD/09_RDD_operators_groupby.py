# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 09_RDD_operators_groupby
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 11:37
@Email  : yangzy927@qq.com
@Desc   ：groupBy传入的函数，按照这个函数来分组
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('a', 1), ('b', 1)])

    # 通过groupby对数据进行分组
    rdd1 = rdd.groupBy(lambda x: x[0])

    print(rdd1.collect())
    print(rdd1.map(lambda x:(x[0],list(x[1]))).collect())
