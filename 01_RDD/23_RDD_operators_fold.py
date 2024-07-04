# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 23_RDD_operators_fold
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 17:48
@Email  : yangzy927@qq.com
@Desc   ：fold，功能和reduce类似，聚合时对每个分区内设置初始值，分区间也设置初始值，很少用
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 11), 3)

    print(rdd.glom().collect())

    # 三个分区内部初始加上10，最后对分区间进行聚合时，也加上了初始值
    print(rdd.fold(10, lambda a, b: a + b))
