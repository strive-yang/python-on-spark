# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 19_RDD_operators_demo
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 14:55
@Email  : yangzy927@qq.com
@Desc   ：针对之前学习算子的总结案例
读取data文件中的order.txt文件，提取北京的数据，
组合北京和商品类别进行输出，同时对结果进行去重，得到北京售卖的商品类别的信息。
=================================================="""
from pyspark import SparkConf, SparkContext
import json
if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    # 读取数据文件
    data_rdd = sc.textFile('../data/input/order.text')

    # 按照|符号进行分割
    json_rdd = data_rdd.flatMap(lambda x: x.split("|"))

    # 将json对象转换为字典对象
    dict_rdd=json_rdd.map(lambda x:json.loads(x))

    # 将北京数据进行过滤，保留北京
    beijing_rdd=dict_rdd.filter(lambda d:d['areaName']=='北京')

    # 组合北京和商品类型
    category_rdd=beijing_rdd.map(lambda x:x['areaName']+'_'+x['category'])

    # 对结果集进行去重
    result_rdd=category_rdd.distinct()

    print(result_rdd.collect())