# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-22 9:59
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, FloatType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def read_data(path, schema, data_type='csv', header='true'):
    """读取数据函数"""

    data = spark.read.format(data_type). \
        option('header', header). \
        schema(schema). \
        load(path)
    return data


if __name__ == '__main__':
    # 构建sparkSession对象
    spark = SparkSession.builder. \
        appName('begin'). \
        master('local[*]'). \
        getOrCreate()
    sc = spark.sparkContext

    # 自定义表结构
    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                              StructField('UnitID', StringType(), True),
                              StructField('IncidentNumber', IntegerType(), True),
                              StructField('CallType', StringType(), True),
                              StructField('CallDate', StringType(), True),
                              StructField('WatchDate', StringType(), True),
                              StructField('CallFinalDisposition', StringType(), True),
                              StructField('AlarmDtTm', StringType(), True),
                              StructField('Adress', StringType(), True),
                              StructField('City', StringType(), True),
                              StructField('Zipcode', IntegerType(), True),
                              StructField('Battalion', StringType(), True),
                              StructField('StationArea', StringType(), True),
                              StructField('Box', StringType(), True),
                              StructField('OriginalPriority', StringType(), True),
                              StructField('Priority', StringType(), True),
                              StructField('FinalPriority', IntegerType(), True),
                              StructField('ALSUnit', StringType(), True),
                              StructField('CallTypeGroup', StringType(), True),
                              StructField('NumAlarms', IntegerType(), True),
                              StructField('UnitType', StringType(), True),
                              StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                              StructField('FirePreventionDistrict', StringType(), True),
                              StructField('SupervisorDistrict', StringType(), True),
                              StructField('Neighborhood', StringType(), True),
                              StructField('Location', StringType(), True),
                              StructField('RowID', StringType(), True),
                              StructField('Delay', FloatType(), True)])

    # 数据位置，读取数据集
    data_path = '../data/learning-spark-v2/sf-fire/sf-fire-calls.csv'
    data = read_data(data_path, fire_schema, 'csv', 'true')
    # data.printSchema()
    # data.show(5,truncate=False)

    # todo 1 筛选数据中的部分特征
    # 筛选出CallType!='Medical Incident'
    data.select('IncidentNumber','AlarmDtTm','CallType').\
        where(data['CallType']!='Medical Incident').\
        show(5,truncate=False)

    # todo 2 筛选报警类型的数量，和具体类型
    data.select('CallType').\
        where(data['CallType'].isNotNull()).\
        agg(F.countDistinct('CallType').alias('DistinctCallTypes')).\
        show()
    data.select('CallType').\
        where(data['CallType'].isNotNull()).\
        distinct().\
        show(10,truncate=False)

    # todo 3 修改列名Delay为ResponseDelayedinMins
    # dataframe是不可变得，所以在修改列名后需要生成新得对象
    new_data=data.withColumnRenamed('Delay','ResponseDelayedinMins')
    new_data.select('ResponseDelayedinMins').\
        where(new_data['ResponseDelayedinMins']>5).\
        show(10,truncate=False)

    # todo 4 将string转换为spark支持的时间戳或者日期
    # 用格式字符串'MM/dd/yyyy'\'MM/dd/yyyy hh:mm:ss a'指定何使的格式来转换数据类型
    ts_data=new_data.withColumn('IncidentDate',F.to_timestamp(new_data['CallDate'],'MM/dd/yyyy')).\
        drop('CallDate').\
        withColumn('OnWatchDate',F.to_timestamp(new_data['WatchDate'],'MM/dd/yyyy')).\
        drop('WatchDate').\
        withColumn('AvailableDtTS',F.to_timestamp(new_data['AlarmDtTm'],'MM/dd/yyyy hh:mm:ss a')).\
        drop('AlarmDtTm')
    ts_data.show(10,truncate=False)
    ts_data.printSchema()

    # todo 5 最常见的消防报警类型
    # 统计所有报警类型出现的次数
    data.select('CallType').\
        where(data['CallType'].isNotNull()).\
        groupBy(data['CallType']).\
        count().\
        orderBy('count',ascending=False).\
        show(10,truncate=False)

    # todo 6 统计报警总数，平均响应时间，最短响应时间和最长响应时间
    ts_data.select(F.sum('NumAlarms'),F.avg('ResponseDelayedinMins'),
                   F.min('ResponseDelayedinMins'),F.max('ResponseDelayedinMins')).\
        show()