# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-08-07 16:18
@Email  : yangzy927@qq.com
@Desc   ：
=================================================="""
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler,OneHotEncoderEstimator,StringIndexer,RFormula
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

if __name__ == '__main__':
    # 初始化环境
    spark=SparkSession.builder.\
        master('local[*]').\
        appName('spark.ml'). \
        config("spark.dynamicAllocation.enabled", "True"). \
        getOrCreate()

    # 加载数据集
    path='file:///home/zhenyu/tmp/pycharm_project_345/data/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet'
    airbnbDF=spark.read.format('parquet').load(path)
    print(airbnbDF.columns)

    airbnbDF.select("neighbourhood_cleansed","room_type","bedrooms","bathrooms","number_of_reviews","price").\
        show(10,0)

    # TODO 1 简单的流水线创建
    # 随机划分数据集
    trainDF,testDF=airbnbDF.randomSplit([.8,.2],seed=42)
    print(f"""训练集大小{trainDF.count()},测试集大小{testDF.count()}""")

    # 将单个特征转换为单个列
    vecAssembler=VectorAssembler(inputCols=['bedrooms'],outputCol='features')
    vecTrainDF=vecAssembler.transform(trainDF)
    vecTrainDF.select("bedrooms","features","price").show(10,0)

    # 构建回归模型
    lr=LinearRegression(featuresCol='features',labelCol='price')
    lrModel=lr.fit(vecTrainDF)
    m=round(lrModel.coefficients[0],2)
    b=round(lrModel.intercept,2)
    print(f"""线性回归模型如下：price={m}*bedrooms+{b}""")

    # 创建流水线
    pipeline=Pipeline(stages=[vecAssembler,lr])
    pipelineModel=pipeline.fit(trainDF)

    # 预测
    predDF=pipelineModel.transform(testDF)
    predDF.select("bedrooms","price","prediction").show(10,0)

    # TODO 2 复杂流水线创建
    # 对数据集中所有离散特征进行编码
    # StringIndString符串转换为索引，OneHotEncoderEstimator将索引转换为二维独热编码
    # 值得注意的是，StringIndexer在spark2.4.0中只支持单列，3.0版本中才支持多列
    # 因此在这里需要用到循环来处理多列；OneHotEncoderEstimator可以处理多列
    categoricalCols=[field for (field,dataType) in trainDF.dtypes if dataType=="string"]
    indexOutputCols=[x+"Index" for x in categoricalCols]
    oheOutputCols=[x+"OHE" for x in categoricalCols]
    stringIndexer=[]
    for i in range(len(categoricalCols)):
        stringIndexer.append(StringIndexer(inputCol=categoricalCols[i],
                                    outputCol=indexOutputCols[i],
                                    handleInvalid="skip"))
    oheEncoder=OneHotEncoderEstimator(inputCols=indexOutputCols,
                                    outputCols=oheOutputCols)
    
    # 找到所有连续特征【不包含labelCol】
    numericalCols=[field for (field,dataType) in trainDF.dtypes
               if ((dataType=="double")&(field!="price"))]
    
    # 将编码后的列和连续型列合并
    assemblerInputs=oheOutputCols+numericalCols
    vecAssembler=VectorAssembler(inputCols=assemblerInputs,outputCol="features") # 转化为单个列

    # 该方法来自于R，可以自动对所有列进行stringIndexer和独热编码操作【double型】。
    # 并使用VectorAssembler将所有列合并为单个向量，因此该方法可以简化以上编码步骤
    rFormula=RFormula(formula="price ~ .",
                  featuresCol="features",
                  labelCol="price",
                  handleInvalid="skip")

    # 创建多过程流水线
    stringIndexer.extend([oheEncoder,vecAssembler,lr])
    # pipeline=Pipeline(stages=stringIndexer)
    # 使用Rformula
    pipeline=Pipeline(stages=[rFormula,lr])

    pipelineModel=pipeline.fit(trainDF)
    predDF=pipelineModel.transform(testDF)
    predDF.select("features","price","prediction").show(10,0)

    # 回归效果评估
    regressionEvaluator=RegressionEvaluator(predictionCol='prediction',
                                        labelCol='price',
                                        metricName='rmse')
    rmse=regressionEvaluator.evaluate(predDF)
    print(f"均方根误差为{rmse:.2f}")

    # 重新指定评价指标为r2，不用重新创建评估器regressionEvaluator
    r2=regressionEvaluator.setMetricName('r2').evaluate(predDF)
    print(f"R2为{r2}")

    # 保存
    pipelinePath="file:///home/zhenyu/tmp/lr-piprline-model"
    pipelineModel.write().overwrite().save(pipelinePath)

    # 加载模型
    # savedPipelineModel=PipelineModel.load(pipelinePath)
