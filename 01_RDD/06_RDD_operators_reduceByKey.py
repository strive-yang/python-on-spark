# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 06_RDD_operators_reduceByKey
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-02 17:33
@Email  : yangzy927@qq.com
@Desc   ：针对KV型数据，按照key分组，对组内数据进行聚合
=================================================="""
from pyspark import SparkConf,SparkContext
if __name__ == '__main__':
    conf=SparkConf().setMaster('local[*]').setAppName('test')
    sc=SparkContext(conf=conf)

    rdd=sc.parallelize([('hadoop',1),('spark',1),('yarn',1),
                    ('hadoop',1),('spark',1),('hadoop',1),
                    ('python',1),('hadoop',1)])
    rdd2=rdd.reduceByKey(lambda a,b:a+b)
    print(rdd.collect())