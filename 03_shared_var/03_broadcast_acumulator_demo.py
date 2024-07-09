# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 03_broadcast_acumulator_demo
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-09 15:07
@Email  : yangzy927@qq.com
@Desc   ：一个关于广播变量和累加器的案例
=================================================="""
import re

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1、读取文件
    file_rdd = sc.textFile('../data/input/accumulator_broadcast_data.txt')

    # 特殊字符的list定义
    abnormal_char = [",", ".", "!", "#", "$", "%"]

    # 2、将特殊字符包装为广播变量
    broadcast = sc.broadcast(abnormal_char)

    # 3、特殊字符出现次数定义累加器
    accumulator = sc.accumulator(0)

    # 4、处理数据，取出数据里的空行，有内容为True，空行删除空格后没内容为False，可以被过滤掉
    lines_rdd = file_rdd.filter(lambda x: x.strip())

    # 5、取出前后空格
    data_rdd = lines_rdd.map(lambda x: x.strip())

    # 6、对数据进行切分，按照正则表达式，空格分隔符存在单个或多个
    # 正则表达式 \s+ 表示不确定空格数量，最少一个
    word_rdd = data_rdd.flatMap(lambda x: re.split("\s+", x))

    # 7、过滤过程中对特殊符号进行计数
    def filter_func(data):
        global accumulator
        # 取广播变量中的特殊符号列表
        abnormal = broadcast.value
        # 如果是特殊符号，累加器+1，返回False，过滤掉
        if data in abnormal:
            accumulator += 1
            return False
        else:
            return True

    normal_word_rdd = word_rdd.filter(filter_func)

    # 对正常单词进行计数
    res_rdd = normal_word_rdd.map(lambda x: (x, 1)). \
        reduceByKey(lambda a, b: a + b)

    print("正常单词计数：",res_rdd.collect())
    print("异常符号计数：",accumulator)