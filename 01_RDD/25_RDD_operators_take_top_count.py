# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 25_RDD_operators_take_top_count
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 18:00
@Email  : yangzy927@qq.com
@Desc   ：take，action算子，取出rdd中的多个元素。
top，action算子，降序排列，取rdd前几个有元素
count，action算子，计算rdd中元素的个数
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    # 取前五个元素
    rdd = sc.parallelize(range(1, 11))
    print(rdd.take(5))

    # 降序排列取前三个元素
    rdd2 = sc.parallelize([4, 1, 2, 5, 87, 22, 34])
    print(rdd2.top(3))

    # 计算rdd中元素的个数
    print(rdd.count())
