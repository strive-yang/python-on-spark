# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 01_broadcast
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-09 11:57
@Email  : yangzy927@qq.com
@Desc   ：定义广播变量，使得在调用外部变量时，避免多次调用造成多次网络oi
spark给每个executor共享一个广播变量。
使用场景：
本地集合对象 和 分布式集合对象（rdd）进行关联的时候
需要对本地集合对象封装为广播变量
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 本地list
    stu_info_list = [(1, '张大仙', 11), (2, '王晓晓', 13), (3, '张甜甜', 11), (4, '王大力', 11)]

    # rdd数据
    score_info_rdd = sc.parallelize([(1, '语文', 99), (2, '数学', 99), (3, '英语', 99), (4, '编程', 99),
                                     (1, '语文', 99), (2, '编程', 99), (3, '语文', 99), (4, '英语', 99),
                                     (1, '语文', 99), (3, '英语', 99), (2, '编程', 99)])

    # 1、标记本地对象为广播变量
    broadcast = sc.broadcast(stu_info_list)


    def map_fun(data):
        id = data[0]
        name = ""
        # 匹配本地id-人名表，匹配成功获得相应的名字
        # 2、从广播变量取出需要的变量
        for i in broadcast.value:
            if id == i[0]:
                name = i[1]

        return (name, data[1], data[2])


    print(score_info_rdd.map(map_fun).collect())
