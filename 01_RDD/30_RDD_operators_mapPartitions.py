# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 30_RDD_operators_mapPartitions
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 11:32
@Email  : yangzy927@qq.com
@Desc   ：mapPartitions，分区操作算子，一次传递一整个分区的数据
分区级计算。减少网络传播时间，性能优于普通的map
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
        return result

    print(rdd.mapPartitions(process).collect())
