# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 29_RDD_operators_saveAsTextFIle
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 10:58
@Email  : yangzy927@qq.com
@Desc   ：saveAsTextFile，将rdd数据写入文件，支持本地、hdfs输出
executor并行执行不需要返回到driver
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([3, 1, 2, 4, 6, 5, 10],3)

    # 将文件保存在本地或者远程集群，out1为文件夹，包含每个分区相关数据的文件。
    print("各分区数据: ",rdd.glom().collect())
    rdd.saveAsTextFile("../data/input/out1")    # 若文件夹重复会报错
    print("保存成功")

    # 将文件保存到hdfs
    rdd.saveAsTextFile('hdfs://node1:8020/output/out1')