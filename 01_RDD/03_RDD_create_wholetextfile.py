# @File     : 03_RDD_create_wholetextfile.py
# @Author   : strive_yang
# @Time     : 2024-07-02 15:36
# @Software : PyCharm
# @Desc     : 第二种rdd读取外部数据元的方法，适合多个小文件。
from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf=SparkConf().setMaster('local[*]').setAppName('test')
    sc=SparkContext(conf=conf)

    # whole textfile 读取一堆小文件夹，key是文件名，value是文件内容。
    rdd=sc.wholeTextFiles('../data/input/tiny_files')
    print(rdd.map(lambda x:x[1]).collect())