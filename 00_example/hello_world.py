# @File : hello_world.py
# @Author : strive_yang
# @Time : 2024-06-28 17:29
# @Software : PyCharm
from pyspark import SparkConf,SparkContext
import os
# 确定pyspark运行时的环境
# os.environ['PYSPARK_PYTHON']="E:\\programfiles\\anaconda\\python.exe"

if __name__ == '__main__':
    conf=SparkConf().setAppName("wordcount helloworld")
    # 通过conf对象构建spark context对象
    sc=SparkContext(conf=conf)

    # TODO：word count单词计数，读取hdfs上的words.txt文件，对文件中的单词进行统计
    # 读取文件
    file_rdd=sc.textFile("hdfs://node1:8020/input/words.txt")

    # 对单词进行切割，得到一个存储全部单词的集合
    words_rdd=file_rdd.flatMap(lambda line:line.split(" "))

    # 将单词转换为元组对象，key 单词，value 数字1
    words_with_one_rdd=words_rdd.map(lambda x:(x,1))

    # 按照key分组，对value执行据合操作
    result_rdd=words_with_one_rdd.reduceByKey(lambda x,y:x+y)

    # 通过collect方法收集rdd
    print(result_rdd.collect())
