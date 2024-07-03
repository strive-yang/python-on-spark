# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 07_RDD_Operators_mapvalues
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 11:20
@Email  : yangzy927@qq.com
@Desc   ：针对二元元组的values进行操作
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop', 2), ('spark', 3), ('yarn', 4)])
    print(rdd.map(lambda x: (x[0], x[1] * 10)).collect())

    print(rdd.mapValues(lambda x: x * 10).collect())
