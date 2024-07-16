# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 14_dataframe_write
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-12 15:45
@Email  : yangzy927@qq.com
@Desc   ：将dataframe写出
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

    # write text写出，只能写出一整个列的数据，需要将df转换为单列的df
    df.select(F.concat_ws('---','user_id','movie_id','rank','ts')).\
        write.\
        mode('overwrite').\
        format('text').\
        save('../data/output/sql/text')

    # write csv
    df.write.mode('overwrite').\
        format('csv').\
        option('sep',';').\
        option('header',True).\
        save('../data/output/sql/csv')

    # write json
    df.write.mode('overwrite'). \
        format('json'). \
        save('../data/output/sql/json')

    # write parquet
    df.write.mode('overwrite'). \
        format('parquet'). \
        save('../data/output/sql/parquet')