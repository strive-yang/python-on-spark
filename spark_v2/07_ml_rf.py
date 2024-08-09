# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-08-08 16:51
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler, OneHotEncoderEstimator, StringIndexer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.tuning import ParamGridBuilder,CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
import time
if __name__ == '__main__':
    start_time=time.time()
    spark = SparkSession.builder. \
        master('local[*]'). \
        appName('ml.hyperparameter.tuning'). \
        config("spark.debug.maxToStringFields", "100"). \
        getOrCreate()

    # todo 1 加载数据集class
    path = 'file:///home/zhenyu/tmp/pycharm_project_345/data/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet'
    airbnbDF = spark.read.format('parquet').load(path)

    # 随机划分数据集
    trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)

    # todo 2 数据处理
    # 离散数据列传给StringIndexer
    categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
    indexOutputCols = [x + "Index" for x in categoricalCols]

    stringIndexer = []
    for i in range(len(categoricalCols)):
        stringIndexer.append(StringIndexer(inputCol=categoricalCols[i],
                                           outputCol=indexOutputCols[i],
                                           handleInvalid="skip"))

    # 数值型数据列
    numericCols = [field for (field, dataType) in trainDF.dtypes
                   if ((dataType == 'double') and (field != 'price'))]

    # 将离散和连续特征相结合
    assemblerInputs = indexOutputCols + numericCols
    vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

    # todo 3 构建随机森林模型
    rf = RandomForestRegressor(labelCol='price', maxBins=40, seed=42)
    stage = stringIndexer + [vecAssembler, rf]
    pipeline = Pipeline(stages=stage)

    # 对maxDepth和numTrees进行网格搜索
    paramGrid = (ParamGridBuilder().
                 addGrid(rf.maxDepth, [2, 4, 6]).
                 addGrid(rf.numTrees, [10, 100]).
                 build())

    # 模型评估
    evaluator=RegressionEvaluator(labelCol='price',
                                  predictionCol='prediction',
                                  metricName='rmse')

    # 进行k折交叉验证【将流水线放入交叉验证器】 会为每个模型执行遍流水线工作
    # cv=CrossValidator(estimator=pipeline,
    #                   evaluator=evaluator,
    #                   estimatorParamMaps=paramGrid,
    #                   numFolds=3,
    #                   seed=42)
    # cvModel=cv.setParallelism(5).fit(trainDF)
    # cvResult=list(zip(cvModel.getEstimatorParamMaps(),cvModel.avgMetrics))
    # print(f"交叉验证结果\n{cvResult}")


    # 优化方案【将交叉验证放入流水线】，
    cv=CrossValidator(estimator=rf,
                      evaluator=evaluator,
                      estimatorParamMaps=paramGrid,
                      numFolds=3,
                      parallelism=5,
                      seed=42)
    stage=stringIndexer+[vecAssembler,cv]
    pipeline=Pipeline(stages=stage)
    pipeModel=pipeline.fit(trainDF)

    # 查看交叉验证阶段相关结果
    cvModel=pipeModel.stages[-1]
    print(pipeModel.stages)
    print(type(pipeModel.stages))
    cvResult=list(zip(cvModel.getEstimatorParamMaps(),cvModel.avgMetrics))
    # print(f"交叉验证结果\n{cvResult}")

    print('程序用时：\n',round(time.time()-start_time,4),'s')
