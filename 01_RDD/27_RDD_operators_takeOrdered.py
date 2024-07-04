# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 27_RDD_operators_takeOrdered
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 10:46
@Email  : yangzy927@qq.com
@Desc   ：takeOrdered，action算子，对RDD进行排序，取前几个数据
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([3, 1, 2, 4, 6, 5, 10])

    # 参数1：数据个数
    # 参数2：对排序的数据进行更改，默认升序
    print(rdd.takeOrdered(3))

    print(rdd.takeOrdered(3,lambda x:-x))   # 利用参数2实现降序