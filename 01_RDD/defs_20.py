# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> defs_20
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-03 15:32
@Email  : yangzy927@qq.com
@Desc   ：针对案例20，尝试调用文件中的其他函数，实现相同功能
=================================================="""
def city_and_category(data):
    return data['areaName']+'_'+data['category']