# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 20_window_function
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-16 11:20
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

    rdd = sc.parallelize([
        ('张三', 'class-1', 98),
        ('李四', 'class-4', 68),
        ('欧阳修', 'class-1', 92),
        ('王安石', 'class-2', 93),
        ('神龙大侠', 'class-3', 88),
        ('李白', 'class-2', 90),
        ('杜甫', 'class-1', 78),
        ('李清照', 'class-4', 99)
    ])
    schema = StructType(). \
        add('name', StringType(), nullable=True). \
        add('class', StringType(), nullable=True). \
        add('score', IntegerType(), nullable=True)
    df = rdd.toDF(schema=schema)

    df.createTempView('stu')

    # TODO 聚合窗口函数的演示
    spark.sql("""select * ,avg(score) over() as avg_score from stu""").show()

    # TODO 排序窗口函数计算
    # rank over\dense_rank over\row_number over
    spark.sql('''
        select *, row_number() over(order by score desc) as row_number_rank,
        dense_rank() over(partition by class order by score desc) as dense_rank,
        rank() over(order by score) as rank
        from stu
        ''').show()

    # TODO NTILE
    spark.sql('select *,ntile(6) over(order by score desc) from stu').show()