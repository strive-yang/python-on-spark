# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 10_RDD_operators_filter
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 11:44
@Email  : yangzy927@qq.com
@Desc   ：filter 过滤想要的数据，返回rdd
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd=sc.parallelize(range(1,7))

    # 使用filter过滤奇数
    filter_rdd=rdd.filter(lambda x:x%2==1)

    print((filter_rdd.collect()))