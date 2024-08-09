# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-25 11:02
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
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
    spark = SparkSession.builder. \
        appName('spark sql'). \
        master('local'). \
        getOrCreate()
    sc = spark.sparkContext

    # 读取数据
    schema = "date STRING,delay INT,distance INT,origin STRING,destination STRING"
    data = read_data('../data/learning-spark-v2/flights/departuredelays.csv', schema, 'csv', 'True')

    # 创建临时视图
    data.createTempView('user_delay')

    # todo 1 找出所有航程超过1000英里的航班
    # sql 风格
    spark.sql("""select distance,origin,destination
        from user_delay where distance>1000
        order by distance desc"""). \
        show(20, truncate=False)
    # dsl风格
    data.select(["distance", "origin", "destination"]). \
        where("distance>1000"). \
        orderBy('distance', ascending=False).show(20, truncate=False)

    # todo 2 找出sfo和ord两座机场间所有延迟时间超过两小时的航班
    spark.sql("""select date,delay,origin,destination
        from user_delay where delay>120 and origin='SFO' and destination='ORD'
        order by delay desc""").show(10)

    # todo 3 利用case语句对延迟航班时间划分不同的延误程度
    spark.sql("""select delay,origin,destination,
        case
            when delay >= 360 then 'very long delay'
            when delay >= 120 and delay < 360 then 'long delay'
            when delay >= 60 and delay < 120 then 'short delay'
            when delay > 0 and delay < 60 then 'tolerable delay'
            when delay = 0 then 'no delay'
            else 'early'
        end as flight_delay
        from user_delay order by origin,delay desc""").show(30)

    # todo 4 将date列转换为可读性更强的格式
    # data_ts = data.withColumn('date', F.date_format('date', 'mm-dd HH:mm'))
    # data_ts.show(10)
    print(data.repartition(10).rdd.getNumPartitions())
