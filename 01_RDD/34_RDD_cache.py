# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 34_RDD_cache
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-04 15:36
@Email  : yangzy927@qq.com
@Desc   ：RDD的缓存技术：cache|persist
可以在集群中的4040节点查看具体的缓存node1:4040，需要连接服务器解释器
需将文件deployment到集群
=================================================="""
import time
from pyspark.storagelevel import StorageLevel
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('test')
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile('../data/input/words.txt')
    rdd2 = rdd1.flatMap(lambda x: x.split(' '))
    rdd3 = rdd2.map(lambda x: (x, 1))

    rdd3.cache()
    # rdd3.persist(StorageLevel.MEMORY_AND_DISK)      # 缓存到内存和硬盘

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())  # 执行后rdd1-4将全部删除

    rdd5=rdd3.groupByKey()
    rdd6=rdd5.mapValues(lambda x:sum(x))
    print(rdd6.collect())

    rdd3.unpersist()  # 解除缓存，注意释放
    time.sleep(100000)