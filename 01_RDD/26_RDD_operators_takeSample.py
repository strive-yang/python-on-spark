# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 26_RDD_operators_takeSample
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 10:40
@Email  : yangzy927@qq.com
@Desc   ：takeSample，action算子，随机取样rdd数据
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd=sc.parallelize(range(1,101))
    # 参数1：True表示可以重复取样，False表示不允许。
    # 参数2：表示采样数
    # 参数3：随机数种子
    print(rdd.takeSample(False, 5, seed=666))