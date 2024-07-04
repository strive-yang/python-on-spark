# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 35_RDD_chechpoint
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 16:17
@Email  : yangzy927@qq.com
@Desc   ：checkpoint，对rdd数据进行集中缓存，需要连接服务器解释器，才能缓存到hdfs
需将文件deployment到集群
=================================================="""
import time
from pyspark.storagelevel import StorageLevel
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    # 1、告知spark开启checkpoint
    sc.setCheckpointDir('hdfs://node1:8020/output/ckp')
    rdd1 = sc.textFile('../data/input/words.txt')
    rdd2 = rdd1.flatMap(lambda x: x.split(' '))
    rdd3 = rdd2.map(lambda x: (x, 1))

    # 2、调用checkpoint API保存数据
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())  # 执行后rdd1-4将全部删除

    rdd5=rdd3.groupByKey()
    rdd6=rdd5.mapValues(lambda x:sum(x))
    print(rdd6.collect())

    time.sleep(100000)