# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 12_RDD_operators_union
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:10
@Email  : yangzy927@qq.com
@Desc   ：union 将两个rdd合并返回rdd，该算子不会去重，类型不同也可以合并
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 3])
    rdd2 = sc.parallelize(['a', 'b', 'c'])
    rdd3 = rdd.union(rdd2)
    print(rdd3.collect())
