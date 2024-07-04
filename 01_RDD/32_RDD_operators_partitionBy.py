# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 32_RDD_operators_partitionBy
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 11:45
@Email  : yangzy927@qq.com
@Desc   ：partitionBy，分区操作算子，对rdd进行自定义分区
分区级，传入key,返回值要分区编号int型
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop',1),('spark',1),('python',1),('hadoop',1),('spark',1)])

    def partition(key):
        if key=='hadoop' or key=='spark':
            return 0
        elif key=='spark':
            return 1
        else:
            return 2

    # 参数1：重新分区后分区数
    # 参数2：自定义分区规则，函数，类型无所谓，传入key,返回值要分区编号int型
    print(rdd.partitionBy(3, partition).glom().collect())
