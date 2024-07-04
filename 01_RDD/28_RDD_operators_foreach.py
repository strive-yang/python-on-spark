# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 28_RDD_operators_foreach
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 10:51
@Email  : yangzy927@qq.com
@Desc   ：foreach，action算子，执行提供的逻辑操作（类似map），没返回值
直接从excutor输出，不给driver汇报
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([3, 1, 2, 4, 6, 5, 10])

    # 无返回值，None
    print(rdd.foreach(lambda x: x * 10))

    # 直接从excutor输出，不给driver汇报，并行输出，所以结果会乱序
    print(rdd.glom().collect())
    rdd.foreach(lambda x:print(x*10))