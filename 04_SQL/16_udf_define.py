# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 16_udf_define
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-16 9:52
@Email  : yangzy927@qq.com
@Desc   ：UDF的两种定义方式
=================================================="""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 构建一个rdd
    rdd = sc.parallelize(range(1, 11)).map(lambda x: [x])
    df = rdd.toDF(['num'])


    # TODO 1、方式1 sparksession.udf.register()
    def num_ride_10(data):
        return data * 10


    # 参数1：注册udf的名称，仅仅可以用于sql风格
    # 参数2：udf的处理逻辑，是一个单独的方法
    # 参数3：udf的返回类型，注册时必须声明返回类型。
    # 返回值对象，udf对象，只可以用于DSL语法。
    udf2 = spark.udf.register('udf1', num_ride_10, IntegerType())

    # SQL风格中使用
    # selectExpr 以select的表达式执行，sql风格的表达式（字符串）
    # select方法，接收普通的字符串字段名，或者返回值是column对象的计算
    df.selectExpr('udf1(num)').show()

    # DSL风格
    # 返回值udf对象，如果作为方法使用，传入参数时一定是column对象。
    df.select(udf2(df['num'])).show()

    # TODO 2：方式2 注册仅能用于DSL风格
    udf3 = F.udf(num_ride_10, IntegerType())
    df.select(udf3(df['num'])).show()
