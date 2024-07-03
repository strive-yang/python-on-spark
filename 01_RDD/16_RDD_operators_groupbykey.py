# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 16_RDD_operators_groupbykey
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:31
@Email  : yangzy927@qq.com
@Desc   ：groupbyKey针对kv型，按照key分组
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 3), ('b', 1), ('a', 3), ('b', 1), ('a', 2)])
    rdd2 = rdd.groupByKey()
    print(rdd2.map(lambda x:(x[0],list(x[1]))).collect())
