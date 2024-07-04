# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 31_RDD_operators_foreachPartitions
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 11:39
@Email  : yangzy927@qq.com
@Desc   ：foreachPartition，分区操作算子，和foreach功能一样
分区级操作，无返回值，减少网络传播时间，性能优于普通的foreach
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([3, 1, 2, 4, 6, 5, 10],3)

    def process(iter):
        result=[]
        for i in iter:
            result.append(i*10)
        print(result)

    rdd.foreachPartition(process)