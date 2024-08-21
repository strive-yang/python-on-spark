# -*- coding: UTF-8 -*-
"""=================================================
@IDE    : PyCharm
@Author : Strive Yang
@Email  : yangzy927@qq.com
@Date   : 2024-08-15 14:28
@Desc   :
================================================="""
import pyspark
from pyspark.sql import SparkSession
from pyspark.mllib.regression import LabeledPoint
import numpy as np
import time
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.stat import Statistics
import pandas as pd
import os

os.environ['PYSPARK_PYTHON'] = '/home/zhenyu/anaconda3/bin/python3'

if __name__ == '__main__':
    # 创建spark环境
    spark = SparkSession.builder. \
        master('yarn'). \
        appName('spark.ml.yarn.logit'). \
        config("spark.dynamicAllocation.enabled", "False"). \
        config('spark.sql.shuffle.partitions', 400). \
        config('spark.default.parallelism', 400). \
        config("spark.driver.memory", '8g'). \
        config("spark.executor.cores", "5"). \
        config("spark.executor.memory", '16g'). \
        config("spark.executor.instances", '20'). \
        config("spark.debug.maxToStringFields", '100'). \
        getOrCreate()
    sc = spark.sparkContext

    # 1、读取数据
    file_path = 'hdfs:///user/zhenyu/data/kddcup.data.gz'  # 训练集
    test_file_path = 'hdfs:///user/zhenyu/data/corrected.gz'  # 测试集
    raw_data_rdd = sc.textFile(file_path).repartition(100)
    test_raw_data_rdd = sc.textFile(test_file_path).repartition(50)

    # 指定代码类型，python是动态强类型语言，IDE无法判断函数返回值类型，无法根据参数类型自动补全
    assert isinstance(raw_data_rdd, pyspark.RDD)
    assert isinstance(test_raw_data_rdd, pyspark.RDD)

    # print(raw_data_rdd.getNumPartitions())


    # 2、将标签转为正常|非正常
    def parse_interaction(line):
        line_split = line.split(",")
        # leave_out = [1,2,3,41]
        clean_line_split = line_split[0:1] + line_split[4:41]
        attack = 1.0
        if line_split[41] == 'normal.':
            attack = 0.0
        # LabeledPoint，第一个参数表示样本label，第二个参数表示其他特征
        return LabeledPoint(attack, np.array([float(x) for x in clean_line_split]))


    training_data = raw_data_rdd.map(parse_interaction)
    test_data = test_raw_data_rdd.map(parse_interaction)
    # print(training_data.getNumPartitions())

    # 建立逻辑回归模型
    print("--------------------------- 原始数据 ---------------------------")
    t0 = time.time()
    logit_model = LogisticRegressionWithLBFGS.train(training_data) # 训练模型
    tt = time.time() - t0
    print("Classifier trained in {} seconds".format(round(tt, 3)))

    labels_and_preds = test_data.map(lambda p: (p.label, logit_model.predict(p.features)))
    t0 = time.time()
    test_accuracy = labels_and_preds.filter(lambda a: a[0] == a[1]).count() / float(test_data.count())
    tt = time.time() - t0

    print("Prediction made in {} seconds. Test accuracy is {}\n".format(round(tt, 3), round(test_accuracy, 4)))


    """过滤部分特征"""
    print("--------------------------- 过滤部分特征数据 ---------------------------")
    # 过滤不重要特征
    def parse_interaction_corr(line):
        line_split = line.split(",")
        # leave_out = [1,2,3,25,27,35,38,40,41]
        clean_line_split = line_split[0:1] + line_split[4:25] + line_split[26:27] + \
                           line_split[28:35] + line_split[36:38] + line_split[39:40]
        attack = 1.0
        if line_split[41] == 'normal.':
            attack = 0.0
        return LabeledPoint(attack, np.array([float(x) for x in clean_line_split]))


    corr_reduced_training_data = raw_data_rdd.map(parse_interaction_corr)
    corr_reduced_test_data = test_raw_data_rdd.map(parse_interaction_corr)

    # Build the model
    t0 = time.time()
    logit_model_2 = LogisticRegressionWithLBFGS.train(corr_reduced_training_data)
    tt = time.time() - t0

    print("Classifier trained in {} seconds".format(round(tt, 3)))

    labels_and_preds = corr_reduced_test_data.map(lambda p: (p.label, logit_model_2.predict(p.features)))
    t0 = time.time()
    test_accuracy = labels_and_preds.filter(lambda a: a[0] == a[1]).count() / float(corr_reduced_test_data.count())
    tt = time.time() - t0

    print("Prediction made in {} seconds. Test accuracy is {}\n".format(round(tt, 3), round(test_accuracy, 4)))

    """皮尔逊和卡方检验"""
    print("--------------------------- 特征统计检验结果 ---------------------------")
    feature_names = ["land", "wrong_fragment",
                     "urgent", "hot", "num_failed_logins", "logged_in", "num_compromised",
                     "root_shell", "su_attempted", "num_root", "num_file_creations",
                     "num_shells", "num_access_files", "num_outbound_cmds",
                     "is_hot_login", "is_guest_login", "count", "srv_count", "serror_rate",
                     "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate",
                     "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
                     "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
                     "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate",
                     "dst_host_rerror_rate", "dst_host_srv_rerror_rate"]

    def parse_interaction_categorical(line):
        line_split = line.split(",")
        clean_line_split = line_split[6:41]
        attack = 1.0
        if line_split[41] == 'normal.':
            attack = 0.0
        return LabeledPoint(attack, np.array([float(x) for x in clean_line_split]))

    training_data_categorical = raw_data_rdd.map(parse_interaction_categorical)
    chi = Statistics.chiSqTest(training_data_categorical)
    pd.set_option('display.max_colwidth', 30)
    records = [(result.statistic, result.pValue) for result in chi]
    chi_df = pd.DataFrame(data=records, index=feature_names, columns=["Statistic", "p-value"])
    print(chi_df,'\n')

    """取出不重要特征后"""
    print("--------------------------- 统计分析后数据 ---------------------------")
    def parse_interaction_chi(line):
        line_split = line.split(",")
        # leave_out = [1,2,3,6,19,41]
        clean_line_split = line_split[0:1] + line_split[4:6] + line_split[7:19] + line_split[20:41]
        attack = 1.0
        if line_split[41] == 'normal.':
            attack = 0.0
        return LabeledPoint(attack, np.array([float(x) for x in clean_line_split]))


    training_data_chi = raw_data_rdd.map(parse_interaction_chi)
    test_data_chi = test_raw_data_rdd.map(parse_interaction_chi)

    # Build the model
    t0 = time.time()
    logit_model_chi = LogisticRegressionWithLBFGS.train(training_data_chi)
    tt = time.time() - t0

    print("Classifier trained in {} seconds".format(round(tt, 3)))
    labels_and_preds = test_data_chi.map(lambda p: (p.label, logit_model_chi.predict(p.features)))
    t0 = time.time()
    test_accuracy = labels_and_preds.filter(lambda a: a[0] == a[1]).count() / float(test_data_chi.count())
    tt = time.time() - t0

    print("Prediction made in {} seconds. Test accuracy is {}\n".format(round(tt, 3), round(test_accuracy, 4)))
