# @File : 01_RDD_create_parallelize.py
# @Author : strive_yang
# @Time : 2024-07-01 16:37
# @Software : PyCharm
from pyspark import SparkConf,SparkContext
# import os
# os.environ['PYSPARK_PYTHON']="E:\\programfiles\\anaconda\\python.exe"  # 如若不设置环境变量，就必须声明该行
if __name__ == '__main__':
    # 初始化sparkcontext对象
    conf=SparkConf().setMaster("local[*]").setAppName("test")
    sc=SparkContext(conf=conf)

    # 并行化集合创建rdd，本地集合 --> 分布式
    rdd=sc.parallelize(range(1,10))
    print("默认分区数是：",rdd.getNumPartitions())
    # 指定分区数
    rdd=sc.parallelize(range(1,10),3)
    print("分区数是：", rdd.getNumPartitions())

    # collect 将rdd中的每个分区中的数据，形成一个list对象
    # collect:分布式 --> 本地集合
    print(rdd.collect())