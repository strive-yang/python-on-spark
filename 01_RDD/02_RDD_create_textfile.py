# @File : 02_RDD_create_textfile.py
# @Author : strive_yang
# @Time : 2024-07-02 15:21
# @Software : PyCharm
from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    # 构建sc
    conf=SparkConf().setMaster('local[*]').setAppName("test")
    sc=SparkContext(conf=conf)

    # textfile api读取数据
    # 读取本地数据
    file_rdd1=sc.textFile("../data/input/words.txt")
    print("默认读取分区数：",file_rdd1.getNumPartitions())
    print("文件内容：",file_rdd1.collect())

    # 加最小分区数参数的测试
    file_rdd2=sc.textFile("../data/input/words.txt",3)
    file_rdd3=sc.textFile("../data/input/words.txt",100)
    print("file_rdd2分区数：",file_rdd2.getNumPartitions())
    print("file_rdd3分区数：",file_rdd3.getNumPartitions())

    # 读取hdfs文件测试
    file_rdd4=sc.textFile('hdfs://node1:8020/input/words.txt')
    print("file_rdd4内容：",file_rdd4.collect())