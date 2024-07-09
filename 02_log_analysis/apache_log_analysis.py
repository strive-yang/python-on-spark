# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> apache_log_analysis
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-09 11:08
@Email  : yangzy927@qq.com
@Desc   ：对apache.log文件进行分析
=================================================="""
from pyspark import SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel
if __name__ == '__main__':
    conf=SparkConf().setAppName('apache log analysis').setMaster('local[*]')
    sc=SparkContext(conf=conf)

    # 读取日志信息，对数据进行划分
    file_rdd=sc.textFile('../data/input/apache.log')
    split_file_rdd=file_rdd.map(lambda x:x.split(' '))
    # print(split_file_rdd.collect())

    # 缓存常用rdd
    split_file_rdd.persist(StorageLevel.DISK_ONLY) # 存储到磁盘

    # TODO：需求1，计算当前网站访问的pv(访问次数)
    web_rdd=split_file_rdd.map(lambda x:(x[4],1))
    res1=web_rdd.reduceByKey(lambda a,b:a+b)
    print("所有网站访问次数：",res1.collect())

    # TODO：需求2，计算当访问的UV(访问用户数)
    # 筛选所有用户，去重
    user_rdd=split_file_rdd.map(lambda x:x[1]).distinct(numPartitions=1)
    print('访问用户数为：',user_rdd.count())
    print('用户分别为：',user_rdd.collect())

    # TODO：需求3，计算那些ip访问了网站
    # 将ip与网站一一对应
    ip_with_web_rdd=split_file_rdd.map(lambda x:x[0]+'_'+x[4])
    print('ip访问的网站：',ip_with_web_rdd.distinct(numPartitions=1).collect())

    # TODO：需求4，那个页面访问量最高
    res4 = web_rdd.reduceByKey(lambda a, b: a + b).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).\
        take(1)
    print('访问量最高网站为：',res4[0])
