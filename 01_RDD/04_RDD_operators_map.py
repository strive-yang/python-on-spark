# -*- coding: UTF-8 -*-
""""=================================================
@Project -> File   ：PySpark -> 04_RDD_operators_map
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-02 17:06
@Email  : yangzy927@qq.com
@Desc   ：map，函数映射
=================================================="""
from pyspark import SparkConf,SparkContext
if __name__ == '__main__':
    conf=SparkConf().setMaster('local[*]').setAppName('test')
    sc=SparkContext(conf=conf)

    rdd=sc.parallelize(range(1,7),3)

    # 定义方法作为算子的传入函数体
    def add(data):
        return data*10

    print(rdd.map(add).collect())

    # lambda表达式来写匿名函数
    print(rdd.map(lambda x:x*10).collect())