# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 13_RDD_operators_join
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:13
@Email  : yangzy927@qq.com
@Desc   ：对两个RDD执行join操作，可实现sql的内外连接，只能用于二元元组，
按照二元元组的key关联
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd=sc.parallelize([(1001,'zhangsan'),(1002,'lisi'),(1003,'wangwu'),(1004,'luominghao')])
    rdd2=sc.parallelize([(1001,'销售部'),(1002,'宣传部')])

    # 按照二元元组的key关联
    print(rdd.join(rdd2).collect())

    # 左外连接
    print(rdd.leftOuterJoin(rdd2).collect())

    # 右外连接
    print(rdd.rightOuterJoin(rdd2).collect())