# -*- coding: UTF-8 -*-
"""=================================================
@IDE    : PyCharm
@Author : Strive Yang
@Email  : yangzy927@qq.com
@Date   : 2024-08-21 11:46
@Desc   :
================================================="""
import pyspark
from pyspark.sql import SparkSession
from pyspark.mllib.regression import LabeledPoint
from numpy import array
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from time import time

if __name__ == '__main__':
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

    # 加载数据集
    data_file = 'hdfs:///user/zhenyu/data/kddcup.data.gz'  # 训练集
    raw_data = sc.textFile(data_file)
    print("Train data size is {}".format(raw_data.count()))

    test_data_file = 'hdfs:///user/zhenyu/data/corrected.gz'  # 测试集
    test_raw_data = sc.textFile(test_data_file)
    print("Test data size is {}".format(test_raw_data.count()))

    assert isinstance(raw_data,pyspark.RDD)
    assert isinstance(test_raw_data,pyspark.RDD)

    # 将数据按照逗号分割
    csv_data = raw_data.map(lambda x: x.split(","))
    test_csv_data = test_raw_data.map(lambda x: x.split(","))

    # 取出其中的离散数据，并去重
    protocols = csv_data.map(lambda x: x[1]).distinct().collect()
    services = csv_data.map(lambda x: x[2]).distinct().collect()
    flags = csv_data.map(lambda x: x[3]).distinct().collect()


    # 奖励三数据转换为连续
    def create_labeled_point(line_split):
        # leave_out = [41]
        clean_line_split = line_split[0:41]

        # convert protocol to numeric categorical variable
        try:
            clean_line_split[1] = protocols.index(clean_line_split[1])
        except:
            clean_line_split[1] = len(protocols)

        # convert service to numeric categorical variable
        try:
            clean_line_split[2] = services.index(clean_line_split[2])
        except:
            clean_line_split[2] = len(services)

        # convert flag to numeric categorical variable
        try:
            clean_line_split[3] = flags.index(clean_line_split[3])
        except:
            clean_line_split[3] = len(flags)

        # convert label to binary label
        attack = 1.0
        if line_split[41] == 'normal.':
            attack = 0.0
        # 第一个参数是label，第二个参数是features
        return LabeledPoint(attack, array([float(x) for x in clean_line_split]))


    training_data = csv_data.map(create_labeled_point)
    test_data = test_csv_data.map(create_labeled_point)

    """建立原始数据的决策树模型"""
    print("--------------------------- 原始数据决策树模型 ---------------------------")
    # Build the model
    t0 = time()
    tree_model = DecisionTree.trainClassifier(training_data,
                                              numClasses=2,
                                              categoricalFeaturesInfo={1: len(protocols), 2: len(services),
                                                                       3: len(flags)},
                                              impurity='gini', maxDepth=4, maxBins=100)
    tt = time() - t0
    print("Classifier trained in {} seconds".format(round(tt, 3)))

    predictions = tree_model.predict(test_data.map(lambda p: p.features))
    labels_and_preds = test_data.map(lambda p: p.label).zip(predictions)
    t0 = time()
    test_accuracy = labels_and_preds.filter(lambda a: a[0] == a[1]).count() / float(test_data.count())
    tt = time() - t0

    print("Prediction made in {} seconds. Test accuracy is {}\n".format(round(tt, 3), round(test_accuracy, 4)))

    """查看树结构"""
    print("Learned classification tree model:")
    print(tree_model.toDebugString(),'\n')

    print("Service 0 is {}".format(services[0]))
    print("Service 52 is {}".format(services[52]))

    """三个主要变量的最小分类树：count、dst_bytes 和 flag"""
    print("--------------------------- 三个主要变量的最小决策树模型 ---------------------------")
    def create_labeled_point_minimal(line_split):
        # leave_out = [41]
        clean_line_split = line_split[3:4] + line_split[5:6] + line_split[22:23]

        # convert flag to numeric categorical variable
        try:
            clean_line_split[0] = flags.index(clean_line_split[0])
        except:
            clean_line_split[0] = len(flags)

        # convert label to binary label
        attack = 1.0
        if line_split[41] == 'normal.':
            attack = 0.0

        return LabeledPoint(attack, array([float(x) for x in clean_line_split]))


    training_data_minimal = csv_data.map(create_labeled_point_minimal)
    test_data_minimal = test_csv_data.map(create_labeled_point_minimal)

    # Build the model
    t0 = time()
    tree_model_minimal = DecisionTree.trainClassifier(training_data_minimal, numClasses=2,
                                                      categoricalFeaturesInfo={0: len(flags)},
                                                      impurity='gini', maxDepth=3, maxBins=32)
    tt = time() - t0
    print("Classifier trained in {} seconds\n".format(round(tt, 3)))

    predictions_minimal = tree_model_minimal.predict(test_data_minimal.map(lambda p: p.features))
    labels_and_preds_minimal = test_data_minimal.map(lambda p: p.label).zip(predictions_minimal)
    t0 = time()
    test_accuracy = labels_and_preds_minimal.filter(lambda a: a[0] == a[1]).count() / float(test_data_minimal.count())
    tt = time() - t0
    print("Prediction made in {} seconds. Test accuracy is {}".format(round(tt, 3), round(test_accuracy, 4)))