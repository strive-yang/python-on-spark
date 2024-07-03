# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 18_RDD_operators_sortbykey
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:46
@Email  : yangzy927@qq.com
@Desc   ：相比sortby更为局限，针对kv型数据，按照key排序
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('e', 3), ('b', 3), ('b', 4), ('z', 3), ('h', 1), ('A', 2)])

    # 参数1：True升序
    # 参数2：排序分区
    # 参数3：对key进行排序前的处理，这里排序是将所有key视为小写
    rdd2 = rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda x: str(x).lower())
    print(rdd2.collect())
