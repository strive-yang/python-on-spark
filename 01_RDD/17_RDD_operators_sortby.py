# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 17_RDD_operators_sortby
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:38
@Email  : yangzy927@qq.com
@Desc   ：sort by对rdd数据进行排序，如果需要全局有序，
需要设置排序分区数numPartitions=1
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 11), ('b', 3), ('b', 4), ('a', 3), ('b', 1), ('a', 2)])

    # sortby对rdd排序

    # 对value数字进行排序
    # 参数1，表示按照哪一列进行排序
    # 参数2，True表示升序，False表示降序
    # 参数3，表示排序的分区数
    rdd2=rdd.sortBy(lambda x:x[1],ascending=True,numPartitions=3)
    print(rdd2.collect())

    # 对key进行排序
    rdd3=rdd.sortBy(lambda x:x[0],ascending=True,numPartitions=1)
    print(rdd3.collect())