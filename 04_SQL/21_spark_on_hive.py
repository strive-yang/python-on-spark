# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 21_spark_on_hive
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-17 16:02
@Email  : yangzy927@qq.com
@Desc   ：集群中spark on hive操作，借用hive的元数据服务
需要在spark 和hive中配置hive-site.xml文件
=================================================="""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession，连接spark on hive
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        config('spark.sql.warehouse.dir','hdfs://node1:8020/usr/hive/warehouse').\
        config('hive.metastore.uris','thrift://node1:9083').\
        enableHiveSupport().\
        getOrCreate()
    sc = spark.sparkContext

    spark.sql('select * from student').show()