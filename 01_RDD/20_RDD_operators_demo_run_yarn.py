# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 19_RDD_operators_demo_run_yarn
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 15:12
@Email  : yangzy927@qq.com
@Desc   ：针对之前学习算子的总结案例，在yarn中运行
读取data文件中的order.txt文件，提取北京的数据，
组合北京和商品类别进行输出，同时对结果进行去重，得到北京售卖的商品类别的信息。
=================================================="""
from pyspark import SparkConf, SparkContext
import os
from defs_20 import city_and_category
import json
os.environ['HADOOP_CONF_DIR']='/export/server/hadoop-3.3.0/etc/hadoop'
if __name__ == '__main__':
    conf = SparkConf().setMaster('yarn').setAppName('test-yarn-1')
    # 如果提交到集群运行时，除了主文件还依赖其他文件
    # 需要设置一个参数，告诉spark，还有依赖文件需要同步到集群
    # 参数：spark.submit.pyFiles
    # 参数的值可以是单个py文件或者是zip文件（多个文件时，zip压缩后上传）
    conf.set('spark_submit_file','defs_20.py')
    sc = SparkContext(conf=conf)

    # 读取node1中hdfs文件
    data_rdd = sc.textFile('hdfs://node1:8020/input/order.txt')

    # 按照|符号进行分割
    json_rdd = data_rdd.flatMap(lambda x: x.split("|"))

    # 将json对象转换为字典对象
    dict_rdd=json_rdd.map(lambda x:json.loads(x))

    # 将北京数据进行过滤，保留北京
    beijing_rdd=dict_rdd.filter(lambda d:d['areaName']=='北京')

    # 组合北京和商品类型
    category_rdd=beijing_rdd.map(city_and_category)

    # 对结果集进行去重
    result_rdd=category_rdd.distinct()

    print(result_rdd.collect())