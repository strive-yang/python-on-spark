# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-26 10:35
@Email  : yangzy927@qq.com
@Desc   ：一次关于K-Means聚类的尝试
ps: a 数据中的值存在负值，需要处理
    b 数据中的时间列数据格式不统一，需要模糊处理
    c 每次show，DataFrame都会像RDD一样删除，再次遇到要根据血缘关系重新计算
    因此，为了避免重新计算，节约时间，在必要的时候需要对数据进行缓存persist
    d 该程序在yarn下运行，需要将文件传入hdfs方便所有集群使用
=================================================="""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,IntegerType
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler,StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.storagelevel import StorageLevel
import numpy as np
import pandas as pd
import matplotlib.pylab as plt
import time
import seaborn as sns
if __name__ == '__main__':
    time_start = time.time()  # 记录开始时间
    # 创建环境，spark.default.parallelism，自定义分区大小
    spark = SparkSession.builder. \
        master('yarn'). \
        config('spark.sql.shuffle.partitions', 400). \
        config('spark.default.parallelism', 400). \
        config("spark.driver.memory", '8g'). \
        config("spark.sql.legacy.timeParserPolicy", "LEGACY"). \
        config("spark.executors.num", "5"). \
        config("spark.executors.cores", "5"). \
        config("spark.executor.memory", '24g'). \
        appName('pyspark.for.ml.on.yarn'). \
        getOrCreate()
    sc = spark.sparkContext

    # 如果使用这个来读取excel文件，读取文件时要花很长时间60s
    # df = pd.read_excel(path, header=0, dtype={'InvoiceDate': str})

    # 1、加载数据
    path = 'hdfs:///user/zhenyu/data/online_retail.csv'
    # 定义数据格式
    schema = StructType([StructField('InvoiceNo', StringType(), True),
                         StructField('StockCode', StringType(), True),
                         StructField('Description', StringType(), True),
                         StructField('Quantity', IntegerType(), True),
                         StructField('InvoiceDate', StringType(), True),
                         StructField('UnitPrice', DoubleType(), True),
                         StructField('CustomerID', StringType(), True),
                         StructField('Country', StringType(), True)])
    data = spark.read.format('csv'). \
        option('header', True). \
        schema(schema). \
        load(path)
    print('数据的分区数：', data.rdd.getNumPartitions())
    print("数据大小: ", data.count())
    print("数据中所有客户的数量: ", data.select('CustomerID').distinct().count())

    # 数据稍微处理下，Quantity、UnitPrice为正值
    data=data.withColumn('Quantity',F.abs(data['Quantity'])).\
        withColumn('UnitPrice',F.abs(data['UnitPrice']))

    # 2、各个国家购买商品的数量
    print("所有国家购买商品数量")
    data.groupBy('Country'). \
        agg(F.count('Quantity').alias('buy_num')). \
        orderBy('buy_num', ascending=False).show(truncate=False)
    # data.printSchema()
    # data.show(10,truncate=False)

    # 3、客户最近一次在电子商务平台上购买是什么时候
    # 将InvoiceDate转为spark中的时间戳
    data_ts = data.withColumn('InvoiceDate', F.to_timestamp('InvoiceDate', 'yyyy-MM-dd HH:mm'))
    data_ts.persist(StorageLevel.DISK_ONLY)   # 缓存数据
    # data_ts.show(10)
    print('最晚一次购买记录：')
    data_ts.select(F.max(data_ts['InvoiceDate'])).show()
    print('最早一次购买记录：')
    data_ts.select(F.min('InvoiceDate')).show()

    # 4、为每个客户分配一个值，值越小表示很久没购买过商品，需要重点关注。
    # date-最早的购买时间,使用lit()函数将常量值添加到DataFrame列
    data_recent = data_ts.withColumn('from_date', F.lit('2010-12-01 08:26:00'))
    # data_recent.printSchema()
    # data_recent.show(10,truncate=False)
    # print("----"*40)
    # 使用cast函数，转换数据类型
    data_recent = data_recent.withColumn('from_date', F.to_timestamp('from_date', 'yyyy-MM-dd HH:mm:ss'))
    data_recent = data_recent.withColumn('recency', data_ts["InvoiceDate"].cast("long") - data_recent['from_date'].cast("long"))
    # data_recent.show(10, truncate=False)

    # 5、各个客户最近一次购买的商品
    # max(recency) 表示最近一次购买的时间，将筛选结果与原始数据做左半连接
    # 对'recency','CustomerID'进行左连接，
    customer_recent=data_recent.join(data_recent.groupBy('CustomerID').agg(F.max('recency').alias('recency')),on=['recency','CustomerID'],how='leftsemi')
    # customer_recent.show(10,0)
    # customer_recent.show(10,0)

    # 6、计算各个客户最近一次购买商品的频率
    bug_fre=customer_recent.groupBy('CustomerID').\
        agg(F.count('StockCode').alias('frequency'))

    # 7、将频率值和customer_recent匹配
    customer_buy_fre=customer_recent.join(bug_fre,on='CustomerID',how='left')

    # 8、在之前的基础上计算用户总花费金额
    final_data=customer_buy_fre.join(customer_buy_fre.withColumn('TotalAmount',F.col('Quantity')*F.col('UnitPrice')).\
        groupBy('CustomerID').agg(F.sum('TotalAmount').alias('custom_monetary')),on='CustomerID',how='inner')
    final_data=final_data.select(['recency','frequency','custom_monetary','CustomerID']).distinct()
    # final_data.show(10,0)

    # 9、数据标准化
    # 将三列数据合并为一列
    assemble=VectorAssembler(inputCols=['recency','frequency','custom_monetary'],outputCol='features')
    assemble_data=assemble.transform(final_data)
    # print(assemble_data.head(3))
    # 对合并的数据features进行标准化
    scale=StandardScaler(inputCol='features',outputCol='standardized')
    data_scale=scale.fit(assemble_data)
    data_scale_output=data_scale.transform(assemble_data)
    data_scale_output.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)   # 缓存数据，show后数据会消失，重新生成浪费时间且在训练模型时有bug
    data_scale_output.show(10, truncate=False)

    cost = np.zeros(10)
    evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', metricName='silhouette',
                                    distanceMeasure='squaredEuclidean')

    # 10、肘部法确定具体的聚类个数
    for i in range(2, 10):
        KMeans_algo = KMeans(featuresCol='standardized', k=i)
        KMeans_fit = KMeans_algo.fit(data_scale_output)
        output = KMeans_fit.transform(data_scale_output)
        cost[i] = KMeans_fit.summary.trainingCost
    print(cost)

    df_cost = pd.DataFrame(cost[2:])
    df_cost.columns = ["cost"]
    new_col = range(2, 10)
    df_cost.insert(0, 'cluster', new_col)
    plt.plot(df_cost.cluster, df_cost.cost)
    plt.xlabel('Number of Clusters')
    plt.ylabel('Score')
    plt.title('Elbow Curve')
    plt.show()

    # 11、按照4个类进行聚类，并预测每个样本的类别
    KMeans_algo = KMeans(featuresCol='standardized', k=4)
    KMeans_fit = KMeans_algo.fit(data_scale_output)

    preds = KMeans_fit.transform(data_scale_output)
    preds.persist(StorageLevel.DISK_ONLY)
    preds.show(20, 0)

    df_viz = preds.select('recency', 'frequency', 'custom_monetary', 'prediction')
    df_viz = df_viz.toPandas()
    avg_df = df_viz.groupby(['prediction'], as_index=False).mean()
    print(avg_df.head(10))
    list1 = ['recency', 'frequency', 'custom_monetary']

    for i in list1:
        sns.barplot(x='prediction', y=str(i), data=avg_df)
        plt.show()

    time_end = time.time()  # 记录结束时间
    print("程序运行时间: ", time_end - time_start)
