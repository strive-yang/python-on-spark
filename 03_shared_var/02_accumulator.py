# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 02_accumulator
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-09 14:47
@Email  : yangzy927@qq.com
@Desc   ：
问题背景：直接使用全局变量count对各个分区的执行次数进行累加，由于count被分发到各个分区
但是Driver中的count=0，因此count最终结果还是0。
为了解决以上问题，需要定义count为spark中的累加器，这样不同分区执行时也可以实现累加功能。
=================================================="""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 11), 2)

    # 累加器变量，初始值为0
    count=sc.accumulator(0)

    # 两个分区单独执行一下函数，
    def map_func(data):
        global count
        count += 1
        # print(count)

    rdd2=rdd.map(map_func)
    rdd2.collect()   # 调用之后rdd2不存在

    rdd3=rdd2.map(lambda x:x)  # 需要重新生成rdd2，所以累加器又被执行
    rdd3.collect()
    print(count)
