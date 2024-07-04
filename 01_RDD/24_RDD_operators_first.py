# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 24_RDD_operators_first
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 17:58
@Email  : yangzy927@qq.com
@Desc   ：first，action算子，取出rdd中的第一个元素
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd1=sc.parallelize([1,2,4,5,5])
    print(rdd1.first())

    rdd2=sc.parallelize([('hadoop',1001),('spar',1002)])
    print(rdd2.first())