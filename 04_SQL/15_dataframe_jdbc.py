# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 15_dataframe_jdbc
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-12 17:05
@Email  : yangzy927@qq.com
@Desc   ：将sparksql中的dataframe数据以jdbc的形式保存到本地数据库中
=================================================="""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 0、构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName('test'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 1、读取数据集u.data
    schema = StructType().add('user_id', StringType(), nullable=True). \
        add('movie_id', IntegerType(), nullable=True). \
        add('rank', IntegerType(), nullable=True). \
        add('ts', StringType(), nullable=True)
    df = spark.read.format('csv'). \
        option('sep', '\t'). \
        option('header', False). \
        option('encoding', 'utf8'). \
        schema(schema=schema). \
        load('../data/input/sql/u.data')

    # 1、将数据写入jdbc中，写入到本地的mysql中
    # df.write.mode('overwrite').\
    #     format('jdbc').\
    #     option('url','jdbc:mysql://localhost:3306/strive_yang?useSSL=false&useUnicode=true').\
    #     option('dbtable','movie_data').\
    #     option('user','root').\
    #     option('password','123456').\
    #     save()

    # 2、读取数据
    df2=spark.read.format('jdbc'). \
        option('url', 'jdbc:mysql://localhost:3306/strive_yang?useSSL=false&useUnicode=true'). \
        option('dbtable', 'movie_data'). \
        option('user', 'root'). \
        option('password', '123456'). \
        load()

    df2.printSchema()
    df2.show()