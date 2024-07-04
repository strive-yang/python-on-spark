# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 33_RDD_operators_repartition_and_coalesce
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 14:54
@Email  : yangzy927@qq.com
@Desc   ：repartition，少用，尽量只做分区减少，分区增加会影响并行计算
极大可能导致shuffle，谨慎使用！！！
coalesce，和repartition功能一样，存在安全机制，用这个比较好，增加分区会提示
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([3, 1, 2, 4, 6, 5, 10],3)

    # repartition 修改分区
    # 减少分区
    print(rdd.repartition(1).glom().collect())

    # 增加分区
    print(rdd.repartition(5).glom().collect())

    # coalesce 修改分区
    # 减少分区
    print(rdd.coalesce(1).glom().collect())

    # 增加分区，存在安全机制，需要shuffle=true
    print(rdd.coalesce(5,shuffle=True).glom().collect())
