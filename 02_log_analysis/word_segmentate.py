# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> word_segmentate
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-09 9:33
@Email  : yangzy927@qq.com
@Desc   ：log analysis demo中需要的相关函数
=================================================="""
import jieba

def content_jeba(data):
    """jieba实现分词操作"""
    seg = jieba.cut_for_search(data)
    return list(seg)


def filter_words(data):
    """过滤某些字段"""
    return data not in ['帮', '谷', '客']


def append_words(data):
    """修订关键词内容"""
    if data == '传智播':
        data = '传智播客'
    if data == '院校':
        data = '院校帮'
    if data == '博学':
        data = '博学谷'
    return (data, 1)


def extract_user_and_word(data):
    """传入数据 元组(1001,我要上深圳大学)"""
    user_id = data[0]
    user_content = data[1]
    words = content_jeba(user_content)

    l = []
    for i in words:
        if filter_words(i):
            l.append((user_id + '_' + append_words(i)[0], 1))
    return l
