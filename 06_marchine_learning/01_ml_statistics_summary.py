# -*- coding: UTF-8 -*-
"""=================================================
@IDE    : PyCharm
@Author : Strive Yang
@Email  : yangzy927@qq.com
@Date   : 2024-08-13 11:19
@Desc   :
================================================="""
from pyspark.sql import SparkSession
import numpy as np
from pyspark.mllib.stat import Statistics
from math import sqrt
import pandas as pd

if __name__ == '__main__':
    # 创建环境
    spark = SparkSession.builder. \
        master('local[*]'). \
        appName('pyspark.ml.test'). \
        config("spark.driver.memory", '8g'). \
        config("spark.executors.cores", "5"). \
        config("spark.executor.memory", '28g'). \
        config("spark.executor.instances", '20'). \
        config("spark.kryoserializer.buffer.max", '128m'). \
        getOrCreate()
    sc = spark.sparkContext

    # 加载数据
    file_path = 'file:///home/zhenyu/tmp/pycharm_project_345/data/ml_data/kddcup_data_10_percent_corrected'
    raw_data = sc.textFile(file_path)

    # todo 1 连续特征统计特性
    # 提取连续型数据
    def parse_interaction(line):
        line_split = line.split(",")
        symbolic_index = [1, 2, 3, 41]
        clean_line_split = [item for i, item in enumerate(line_split) if i not in symbolic_index]
        return np.array([float(x) for x in clean_line_split])

    vector_data = raw_data.map(parse_interaction)

    # 计算列汇总统计数据
    # 按列统计各个特征的统计特性
    summary = Statistics.colStats(vector_data)
    print("Duration Statistics:")
    print(" Mean: {}".format(round(summary.mean()[0], 3)))
    print(" St. deviation: {}".format(round(sqrt(summary.variance()[0]), 3)))
    print(" Max value: {}".format(round(summary.max()[0], 3)))
    print(" Min value: {}".format(round(summary.min()[0], 3)))
    print(" Total value count: {}".format(summary.count()))
    print(" Number of non-zero values: {}".format(summary.numNonzeros()[0]))

    # todo 2 label统计特性
    # 保留label
    def parse_interaction_with_key(line):
        line_split = line.split(",")
        # keep just numeric and logical values
        symbolic_indexes = [1, 2, 3, 41]
        clean_line_split = [item for i, item in enumerate(line_split) if i not in symbolic_indexes]
        return (line_split[41], np.array([float(x) for x in clean_line_split]))
    # 包含label的数据，rdd
    label_vector_data = raw_data.map(parse_interaction_with_key)

    # normal的统计特征
    normal_label_data = label_vector_data.filter(lambda x: x[0] == 'normal.')
    normal_summary = Statistics.colStats(normal_label_data.values())
    print("Duration Statistics for label: {}".format("normal"))
    print(" Mean: {}".format(normal_summary.mean()[0], 3))
    print(" St. deviation: {}".format(round(sqrt(normal_summary.variance()[0]), 3)))
    print(print(" Max value: {}".format(round(normal_summary.max()[0], 3))))
    print(" Min value: {}".format(round(normal_summary.min()[0], 3)))
    print(" Total value count: {}".format(normal_summary.count()))
    print(" Number of non-zero values: {}".format(normal_summary.numNonzeros()[0]))

    # 为了方便随时查看各个label的特征，用函数上一封装过程
    # 避免反复生成label_vector_data，直接调用并按要求过滤
    def summary_by_label(label_vector_data, label):
        label_vector_data =label_vector_data.filter(lambda x: x[0] == label)
        return Statistics.colStats(label_vector_data.values())

    normal_summary = summary_by_label(label_vector_data, 'normal.')

    print("Duration Statistics for label: {}".format("normal"))
    print(" Mean: {}".format(normal_summary.mean()[0], 3))
    print(" St. deviation: {}".format(round(sqrt(normal_summary.variance()[0]), 3)))
    print(print(" Max value: {}".format(round(normal_summary.max()[0], 3))))
    print(" Min value: {}".format(round(normal_summary.min()[0], 3)))
    print(" Total value count: {}".format(normal_summary.count()))
    print(" Number of non-zero values: {}".format(normal_summary.numNonzeros()[0]))

    # 查看特征guess_passwd的第一列统计特征
    guess_passwd_summary = summary_by_label(label_vector_data, "guess_passwd.")
    print("Duration Statistics for label: {}".format("guess_password"))
    print(" Mean: {}".format(guess_passwd_summary.mean()[0], 3))
    print(" St. deviation: {}".format(round(sqrt(guess_passwd_summary.variance()[0]), 3)))
    print(" Max value: {}".format(round(guess_passwd_summary.max()[0], 3)))
    print(" Min value: {}".format(round(guess_passwd_summary.min()[0], 3)))
    print(" Total value count: {}".format(guess_passwd_summary.count()))
    print(" Number of non-zero values: {}".format(guess_passwd_summary.numNonzeros()[0]))

    # 所有label组成的list
    label_list = ["back.", "buffer_overflow.", "ftp_write.", "guess_passwd.",
                  "imap.", "ipsweep.", "land.", "loadmodule.", "multihop.",
                  "neptune.", "nmap.", "normal.", "perl.", "phf.", "pod.", "portsweep.",
                  "rootkit.", "satan.", "smurf.", "spy.", "teardrop.", "warezclient.",
                  "warezmaster."]

    # 获得每个label的统计数据对象，key/value
    stats_by_label = [(label, summary_by_label(label_vector_data, label)) for label in label_list]

    # 提取每个label第一列的统计对象
    duration_by_label = [
        (stat[0], np.array([float(stat[1].mean()[0]), float(sqrt(stat[1].variance()[0])), float(stat[1].min()[0]),
                            float(stat[1].max()[0]), int(stat[1].count())]))
        for stat in stats_by_label]
    pd.set_option('display.max_columns', 50)

    stats_by_label_df2 = pd.DataFrame([list(i[1]) for i in duration_by_label], columns=["Mean", "Std Dev", "Min", "Max", "Count"])
    stats_by_label_df2.index = label_list

    print("Duration statistics, by label")
    print(stats_by_label_df2)

    # 将上述功能封装为函数，方便汇总所有label各个列的特征
    def get_variable_stats_df(stats_by_label, column_i):
        column_stats_by_label = [
            (stat[0], np.array([float(stat[1].mean()[column_i]), float(sqrt(stat[1].variance()[column_i])),
                                float(stat[1].min()[column_i]), float(stat[1].max()[column_i]), int(stat[1].count())]))
            for stat in stats_by_label]
        stats_by_label_df = pd.DataFrame([list(i[1]) for i in column_stats_by_label],
                                         columns=["Mean", "Std Dev", "Min", "Max", "Count"])
        stats_by_label_df.index = label_list
        return stats_by_label_df
    print(get_variable_stats_df(stats_by_label, 0))

    # 计算所有连续变量的spearman相关系数
    correlation_matrix = Statistics.corr(vector_data, method="spearman")
    pd.set_option('display.max_columns', 50)

    col_names = ["duration", "src_bytes", "dst_bytes", "land", "wrong_fragment",
                 "urgent", "hot", "num_failed_logins", "logged_in", "num_compromised",
                 "root_shell", "su_attempted", "num_root", "num_file_creations",
                 "num_shells", "num_access_files", "num_outbound_cmds",
                 "is_hot_login", "is_guest_login", "count", "srv_count", "serror_rate",
                 "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate",
                 "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
                 "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
                 "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate",
                 "dst_host_rerror_rate", "dst_host_srv_rerror_rate"]

    corr_df = pd.DataFrame(correlation_matrix, index=col_names, columns=col_names)
    print(corr_df)

    # 筛选相关性高的变量
    highly_correlated_df = (abs(corr_df) > .8) & (corr_df < 1.0)
    correlated_vars_index = (highly_correlated_df == True).any()
    correlated_var_names = correlated_vars_index[correlated_vars_index == True].index
    print('相关性高的变量：')
    print(corr_df.loc[correlated_var_names, correlated_var_names])