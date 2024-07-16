# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> 12_movie_demo
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-12 9:55
@Email  : yangzy927@qq.com
@Desc   ：简单的电影用户评价分析
agg()   可以再内部写多个聚合count、avg等
alias() 可以对agg中的聚合函数直接改名
withColumnRenamed   对DF中的列改名
orderBy 排序，参数1排序的列，参数2ascending降序False
first   df的API，取出df第一行数据，返回row对象（就是一个数组，可以row['列名']调用或者row[下标]调用）
config('spark.sql.shuffle.partitions', 100) sql计算中，默认分区数是200个，可以在这自主设置
与rdd中的分区是相互独立的
=================================================="""
import time
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

    # TODO 1、用户电影评分平均分
    # DSL风格
    print('用户电影评分均分')
    df.groupBy('user_id'). \
        avg('rank'). \
        withColumnRenamed('avg(rank)', 'avg_rank'). \
        withColumn('avg_rank', F.round('avg_rank', 2)). \
        orderBy('avg_rank', ascending=False).show()

    # TODO 2、电影的平均分
    # SQL风格
    print('电影的均分')
    df.createTempView('movie')
    spark.sql(
        'select movie_id,round(avg(rank),2) as avg_rank from movie group by movie_id order by avg_rank desc ').show()

    # TODO  3、查询大于平均分的电影的数量
    # 先计算每个电影的平均分，在和总平均分对比
    df2 = df.groupBy(df['movie_id']). \
        avg('rank'). \
        withColumnRenamed('avg(rank)', 'avg_rank')
    # 和总平均分对比
    print('大于平均分的电影的数量')
    print(df2.where(df2['avg_rank'] > df.select(F.avg(df['rank'])).first()[0]).count())

    # TODO 4、高分电影中找出打分次数最多的人，此人打的平均分
    id = df.where('rank > 3'). \
        groupBy('user_id'). \
        count(). \
        withColumnRenamed('count', 'cnt'). \
        orderBy('cnt', ascending=False). \
        limit(1).first()[0]
    score = df.filter(df['user_id'] == id). \
        select(F.round(F.avg('rank'), 2)).first()[0]
    print('打出高分最多的用户id和平均分')
    print(id, score)

    # TODO 5、查询每个用户的平均打分，最低打分和最高打分
    # 一般一个groupBy后面只能写一个聚合，如果想一次性写多个聚合就要用到聚合函数agg
    df.groupBy('user_id'). \
        agg(
        F.round(F.avg('rank'), 2).alias('avg_rank'),
        F.min('rank').alias('min_rank'),
        F.max('rank').alias('max_rank')
    ).show()

    # TODO 6、查询评价次数超过一百次的电影，该电影的平均分 ，排名Top10
    df.groupBy('movie_id'). \
        agg(
        F.count('movie_id').alias('cnt'),
        F.round(F.avg('rank'), 2).alias('avg_rank')
    ).where('cnt > 100'). \
        orderBy('avg_rank', ascending=False). \
        limit(10). \
        show()

    # time.sleep(100000)