# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 05_RDD_operators_flatmap
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-02 17:19
@Email  : yangzy927@qq.com
@Desc   ：flatmap 解除数组中的嵌套
=================================================="""
from pyspark import SparkConf,SparkContext
if __name__ == '__main__':
    conf=SparkConf().setMaster('local[*]').setAppName('test')
    sc=SparkContext(conf=conf)

    rdd=sc.parallelize(['spark hadoop hadoop','spark yarn yran','hadoop spark hadoop'])
    rdd2=rdd.map(lambda x:x.split(" "))
    print(rdd2.collect())

    rdd3=rdd.flatMap(lambda x:x.split(" "))
    print(rdd3.collect())