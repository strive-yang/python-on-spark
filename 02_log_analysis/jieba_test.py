# -*- coding: UTF-8 -*-
"""=================================================
@Project -> File   ：PySpark -> jieba_test
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-07-08 17:44
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
import jieba
if __name__ == '__main__':
    content='小明硕士毕业于深圳大学，后再香港理工大学深造'

    res=jieba.cut(content,True)
    print(list(res))

    res2=jieba.cut(content,False)
    print(list(res2))

    # 搜索引擎模式，等同于允许二次组合的场景
    res3=jieba.cut_for_search(content)
    print(','.join(res3))