# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-08-08 15:42
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
from pyspark.sql import SparkSession
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler,OneHotEncoderEstimator,StringIndexer,RFormula
from pyspark.ml import Pipeline,PipelineModel
import pandas as pd

if __name__=='__main__':
    spark=SparkSession.builder.\
        master('local[*]').\
        appName('ml.hyperparameter.tuning').\
        config("spark.debug.maxToStringFields", "100").\
        getOrCreate()
    
    # 加载数据集class
    path='file:///home/zhenyu/tmp/pycharm_project_345/data/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet'
    airbnbDF=spark.read.format('parquet').load(path)
    # print(airbnbDF.columns)

    # 随机划分数据集
    trainDF,testDF=airbnbDF.randomSplit([.8,.2],seed=42)
    print(f"""训练集大小{trainDF.count()},测试集大小{testDF.count()}""")

    # 构建决策树模型
    dt=DecisionTreeRegressor(labelCol='price')

    # 离散数据列传给StringIndexer
    categoricalCols=[field for (field,dataType) in trainDF.dtypes if dataType=="string"]
    indexOutputCols=[x+"Index" for x in categoricalCols]

    stringIndexer=[]
    for i in range(len(categoricalCols)):
        stringIndexer.append(StringIndexer(inputCol=categoricalCols[i],
                                    outputCol=indexOutputCols[i],
                                    handleInvalid="skip"))

    # 数值型数据列
    numericCols=[field for (field,dataType) in trainDF.dtypes
                 if ((dataType=='double') and (field != 'price'))]
    
    # 将离散和连续特征相结合
    assemblerInputs=indexOutputCols+numericCols
    vecAssembler=VectorAssembler(inputCols=assemblerInputs,outputCol="features")

    # maxbins确定各个工作节点分叉点的临界值，默认为32.
    # 该值要大于分类列的分类个数

    # 构建流水线
    pipeline=Pipeline(stages=stringIndexer+[vecAssembler,dt])
    # pipelineModel=pipeline.fit(trainDF)   # 直接运行会报错
    dt.setMaxBins(40)
    pipelineModel=pipeline.fit(trainDF)

    # 决策树分叉条件
    dtModel=pipelineModel.stages[-1]
    print(dtModel.toDebugString)

    featureImp=pd.DataFrame(
        list(zip(vecAssembler.getInputCols(),dtModel.featureImportances)),
        columns=['feature','importance'])
    print(featureImp.sort_values(by='importance',ascending=False))