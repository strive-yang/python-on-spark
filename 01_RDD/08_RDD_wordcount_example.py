# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 08_RDD_wordcount_example
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 11:29
@Email  : yangzy927@qq.com
@Desc   ：简单的写一个wordcount案例
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    # 1、读取文件，构建rdd
    file_rdd = sc.textFile('../data/input/words.txt')

    # 2、取出文件中的所有单词
    word_rdd = file_rdd.flatMap(lambda x: x.split(' '))

    # 3、将单词转换为元组，key是单词，value是1
    word_with_one_rdd = word_rdd.map(lambda x: (x, 1))

    # 4、reducebykey单词分组并聚合
    result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 通过collect将结果从分区中发送到driver
    print(result_rdd.collect())
