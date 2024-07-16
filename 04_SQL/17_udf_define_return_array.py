# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 17_udf_define_return_array
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-16 10:19
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
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

    # 创建rdd
    rdd = sc.parallelize([['hadoop python spark'], ['hadoop spark spark']])
    df = rdd.toDF(['line'])


    # 注册udf，udf的执行函数定义，返回值是一个array对象

    def split_line(data):
        return data.split(' ')


    # TODO 1 方式1构建udf
    udf2 = spark.udf.register('udf1', split_line, ArrayType(StringType()))
    # DSL风格
    df.select(udf2(df['line'])).show()
    # SQL风格
    df.createTempView('lines')
    spark.sql('select udf1(line) from lines').show(truncate=False)

    # TODO 2 方式2构建udf
    udf3 = F.udf(split_line,ArrayType(StringType()))
    df.select(udf3(df['line'])).show()

