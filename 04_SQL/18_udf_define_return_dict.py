# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 18_udf_define_return_dict
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-16 10:34
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
import string
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

if __name__ == '__main__':
    # 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 假设有三个数字1、2、3  传入1返回{'num':1,'letters':'a'}
    rdd = sc.parallelize([[1],[2],[3]])
    df=rdd.toDF(['num'])


    # 注册udf
    def process(data):
        return {"num": data, 'letter': string.ascii_letters[data]}


    # udf的返回值是字典的话需要使用StructType来接收
    udf2 = spark.udf.register('udf1', process, StructType().\
                              add('num',IntegerType(),nullable=True).\
                              add('letter',StringType(),nullable=True))
    df.selectExpr('udf1(num)').show(truncate=False)
    df.select(udf2(df['num'])).show(truncate=False)
