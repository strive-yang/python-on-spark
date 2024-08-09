# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-08-01 16:40
@Email  : yangzy927@qq.com
@Desc   ：
需求1 各省销售额统计
需求2 top3销售省份中，有多少店铺达到日销售1000+
需求3 top3省份中，各省平均单价
需求4 top3省份中，各类型支付类型比例

receivable: 订单金额
storeProvince: 店铺省份
dateTS: 订单的销售日期
payType: 支付类型
storeID: 店铺id

两个操作：需要配置集群jar包【没权限|已解决，加入指定的jar包】
1、写出结果到mysql
2、写出结果到hive库
=================================================="""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType
# 配置mysql需要的jar包【仅限local,集群中还是需要加入jar包】
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-class-path /home/zhenyu/anaconda3/lib/python3.6/site-packages/pyspark/jars/mysql-connector-java-5.1.49.jar pyspark-shell'

if __name__ == '__main__':
    spark = SparkSession.builder. \
        master('yarn'). \
        appName('SparkSQL.Example'). \
        config('spark.sql.shuffle.partitions', '2'). \
        config('spark.sql.warehouse.dir', 'hdfs://node01:8020/user/zhenyu/hive/warehouse'). \
        config("hive.metastore.uris", "thrift://node01:9083"). \
        config("spark.debug.maxToStringFields", "100"). \
        config("spark.dynamicAllocation.enabled", "False"). \
        config('spark.sql.shuffle.partitions', 400). \
        config('spark.default.parallelism', 400). \
        config("spark.driver.memory", '8g'). \
        config("spark.executors.cores", "5"). \
        config("spark.executor.memory", '28g'). \
        config("spark.executor.instances", '20'). \
        enableHiveSupport(). \
        getOrCreate()

    # 1、读取数据
    # 数据中省份信息存在缺失，将缺失值过滤；null空字符串也过滤
    # 订单金额，单笔超过10000为测试数据，需要删除
    # 列值裁剪，选择需要的5个列
    df = spark.read.format('json'). \
        load('hdfs:///user/zhenyu/data/mini.json'). \
        dropna(thresh=1, subset=['storeProvince']). \
        filter("storeProvince != 'null'"). \
        filter("receivable < 10000"). \
        select("storeProvince", "storeID", "receivable", "dateTS", "payType")

    # todo 需求1 各个省份销售额
    province_sale_df = df.groupBy('storeProvince').sum("receivable"). \
        withColumnRenamed("sum(receivable)", "money"). \
        withColumn("money", F.round('money', 2)). \
        orderBy('money', ascending=False)
    province_sale_df.show(10, 0)
    # province_sale_df.printSchema()

    # 写出mysql【缺少依赖的jar包|已经解决，加入环境变量就行】
    # province_sale_df.write.mode('overwrite').\
    #     format('jdbc').\
    #     option('url','jdbc:mysql://node01:3306/zhenyu?useSSL=false&useUnicode=true&characterEncoding=utf8').\
    #     option('dbtable','province_sale').\
    #     option('user','zhenyu').\
    #     option('password','123456').\
    #     option('encoding','utf-8').\
    #     save()

    # 写出hive
    province_sale_df.write.mode("overwrite").saveAsTable("zhenyu.province_sale","parquet")

    # todo 需求2 top3销售省份中，有多少店铺达到日销售1000+
    # 2.1 先找到top3的销售省份
    top3_province_df = province_sale_df.limit(3).select('storeProvince')

    # 2.2 和原始df内关联
    top3_province_df_joined = top3_province_df.join(df, on='storeProvince', how='inner')

    top3_province_df_joined.persist(StorageLevel.MEMORY_AND_DISK)

    # from_unixtime的精度是秒级，数据的精度是毫秒级，要对数据的进度进行裁剪
    province_hot_store=top3_province_df_joined.groupBy('storeProvince', 'storeID',
                                    F.from_unixtime(top3_province_df_joined['dateTS'].substr(0, 10), "yyyy-MM-dd").alias('day')). \
        sum("receivable").withColumnRenamed('sum(receivable)', 'money'). \
        filter("money > 1000").\
        dropDuplicates(subset=["storeID"]).\
        groupBy('storeProvince').count()
    province_hot_store.show(10,0)

    # # 写出mysql【缺少依赖的jar包|已经解决，加入环境变量就行】
    # province_hot_store.write.mode('overwrite').\
    #     format('jdbc').\
    #     option('url','jdbc:mysql://node01:3306/zhenyu?useSSL=false&useUnicode=true&characterEncoding=utf8').\
    #     option('dbtable','province_hot_store').\
    #     option('user','zhenyu').\
    #     option('password','123456').\
    #     option('encoding','utf-8').\
    #     save()

    # 写出hive
    province_hot_store.write.mode("overwrite").saveAsTable("zhenyu.province_hot_store","parquet")

    # todo 需求3 top3省，各个省份的单单价
    top3_province_avg=top3_province_df_joined.groupBy('storeProvince').avg('receivable').\
        withColumnRenamed('avg(receivable)','money').\
        withColumn('money',F.round('money',2)).\
        orderBy('money',ascending=False)

    # # 写出mysql【缺少依赖的jar包|已经解决，加入环境变量就行】
    # top3_province_avg.write.mode('overwrite').\
    #     format('jdbc').\
    #     option('url','jdbc:mysql://node01:3306/zhenyu?useSSL=false&useUnicode=true&characterEncoding=utf8').\
    #     option('dbtable','top3_province_avg').\
    #     option('user','zhenyu').\
    #     option('password','123456').\
    #     option('encoding','utf-8').\
    #     save()
    top3_province_avg.show(10,0)

    # 写出hive
    top3_province_avg.write.mode("overwrite").saveAsTable("zhenyu.top3_province_avg","parquet")

    # todo 需求4 top3中各个省份的支付比例
    # 湖南省 支付宝 微信等比例
    top3_province_df_joined.createTempView("province_pay")


    def udf_function(percent):
        return str(round(percent * 100, 2)) + "%"

    # 注册udf【udf API在YARN模式下的调用会导致整个集群调用各自节点的python解释器，配置上会报错】
    my_udf = F.udf(udf_function, StringType())
    payType_df=spark.sql("""
        select storeProvince,payType,(count(payType)/total) as percent from
        (select storeProvince,payType,count(1) over(partition by storeProvince) as total from province_pay) as sub
        group by storeProvince,payType,total
    """).withColumn('percent',my_udf('percent'))

    # payType_df.show(10,0)
    # payType_df.write.mode("overwrite").saveAsTable("zhenyu.payType_df","parquet")

    # # 写出mysql【缺少依赖的jar包|已经解决，加入环境变量就行】
    # payType_df.write.mode('overwrite').\
    #     format('jdbc').\
    #     option('url','jdbc:mysql://node01:3306/zhenyu?useSSL=false&useUnicode=true&characterEncoding=utf8').\
    #     option('dbtable','payType_df').\
    #     option('user','zhenyu').\
    #     option('password','123456').\
    #     option('encoding','utf-8').\
    #     save()

    top3_province_df_joined.unpersist()
