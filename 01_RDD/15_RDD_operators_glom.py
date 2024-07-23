# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 15_RDD_operators_glom
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:26
@Email  : yangzy927@qq.com
@Desc   ：glom将rdd数据，加上嵌套，按照分区，可用于查看数据分区
glom+collect返回值是一个展示各个分区的list
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 11))
    print("分区数为：", rdd.getNumPartitions())
    print(rdd.glom().collect())

    # 解嵌套
    print(rdd.glom().flatMap(lambda x: x).collect())
