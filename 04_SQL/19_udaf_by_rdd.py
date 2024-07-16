# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 19_udaf_by_rdd
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-16 11:06
@Email  : yangzy927@qq.com
@Desc   ：曲线救国，pyspark不提供直接的udaf方法
需要使用rdd来实现udaf函数
=================================================="""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize(range(1, 7), 3)
    df = rdd.map(lambda x: [x]).toDF(['num'])

    # 曲线救国：使用rdd的mapPartitions 算子来完成聚合操作
    # mapPartitions API完成UDAF聚合一定是单分区
    single_partition_rdd = df.rdd.repartition(1)   # 内部存储的是row对象
    # print(single_partition_rdd.collect())

    def process(data):
        s = 0
        for row in data:
            s += row['num']
        return [s]   # 嵌套list,mapPartitions方法要求的返回值是list对象

    print(single_partition_rdd.mapPartitions(process).collect())
