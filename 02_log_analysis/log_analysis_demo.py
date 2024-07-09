# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> log_analysis_demo
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-08 17:51
@Email  : yangzy927@qq.com
@Desc   ：对用户日志进行分析
=================================================="""
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from word_segmentate import content_jeba, filter_words, append_words, extract_user_and_word
from operator import add

if __name__ == '__main__':
    # 0、构建初始化环境，构建sc对象
    conf = SparkConf().setAppName('SoGouQ').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 1、读取文件
    file_rdd = sc.textFile('../data/input/SogouQ.txt')

    # 2、对文件数据进行切分
    split_rdd = file_rdd.map(lambda x: x.split('\t'))

    # 3、因为要做多个需求，split_rdd作为基础rdd，会被多次使用
    split_rdd.persist(StorageLevel.DISK_ONLY)

    # TODO：需求1：用户关键词分析，分析关键词数量
    # 分析主要热点词
    # 将所有搜索内容取出
    # print(split_rdd.takeSample(True, 10))
    search_rdd = split_rdd.map(lambda x: x[2])

    # 对搜索内容进行分词处理
    words_rdd = search_rdd.flatMap(content_jeba)

    filter_rdd = words_rdd.filter(filter_words)
    final_word_rdd = filter_rdd.map(append_words)

    # 对单词进行分组聚合并排序
    result1 = final_word_rdd.reduceByKey(lambda a, b: a + b). \
        sortBy(lambda x: x[1], ascending=False, numPartitions=1). \
        take(5)
    print('关键词Top5: ', result1)

    # TODO：需求2：用户和关键词组合分析
    # 用户和关键词对应，分析用户搜索热点
    user_content_rdd = split_rdd.map(lambda x: (x[1], x[2]))
    # 对用户的搜索内容进行分词，再与id对应
    user_word_with_one_rdd = user_content_rdd.flatMap(extract_user_and_word)

    result2 = user_word_with_one_rdd.reduceByKey(lambda a, b: a + b). \
        sortBy(lambda x: x[1], ascending=False, numPartitions=1). \
        take(5)
    print('用户需求Top5: ', result2)

    # TODO：需求3：热门时间段分析
    # 取出时间
    time_rdd = split_rdd.map(lambda x: x[0])
    # 保留小时精度
    time_with_one_rdd = time_rdd.map(lambda x: (x.split(':')[0], 1))

    result3 = time_with_one_rdd.reduceByKey(add). \
        sortBy(lambda x: x[1], ascending=False, numPartitions=1). \
        take(5)
    print('热门时间段: ', result3)
