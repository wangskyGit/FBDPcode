import sys
from operator import add
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import pandas_udf, PandasUDFType     
from pyspark.mllib.classification import SVMModel,SVMWithSGD
from pyspark.mllib.evaluation import BinaryClassificationMetrics
if __name__ == "__main__":
    sc = SparkContext("local", "most popular item")
    spark = SparkSession.builder \
    .master("local") \
    .appName("task3") \
    .config('spark.sql.crossJoin.enabled','true')\
    .getOrCreate()

    Schema1=StructType(
        [
            StructField('user_id',DoubleType()),
            StructField('seller_id',DoubleType()),
            StructField('age',DoubleType()),
            StructField('gender',DoubleType()),
            StructField('action0',DoubleType()),
            StructField('action1',DoubleType()),
            StructField('action2',DoubleType()),
            StructField('action3',DoubleType()),
            StructField('gender0',DoubleType()),
            StructField('gender1',DoubleType()),
            StructField('gender2',DoubleType())
        ]
    )
    Schema2=StructType(
        [
            StructField('user_id',IntegerType()),
            StructField('seller_id',IntegerType()),
            StructField('label',IntegerType())
        ]
    )
    data=spark.read.format('csv').option('sep',',').option('header','false').load('/home/wangsky/FBDP/processed_data',schema=Schema1)
    label=spark.read.format('csv').option('sep',',').option('header','false').load('/home/wangsky/FBDP/train_format1.csv',schema=Schema2)
    train_data=label.join(data,['user_id','seller_id'],'left')
    t=train_data.drop('gender').dropna(how='any').rdd
    training,testing=t.randomSplit([0.7,0.3],seed=10)
    training=training.map(lambda line: LabeledPoint(line[2],[line[3:]]))
    testing=testing.map(lambda line: LabeledPoint(line[2],[line[3:]]))
    training.cache()
    svm=SVMWithSGD.train(training,iterations=25)
    predicted=training.map(lambda x:(x.label,svm.predict(x.features)))
    print(predicted.take(5))
    error=predicted.filter(lambda lp:lp[0]!=lp[1]).count()/float(training.count())
    print('训练集error：{}'.format(error))
    test_pre=testing.map(lambda x:(x.label,svm.predict(x.features)))
    test_error=predicted.filter(lambda lp:lp[0]!=lp[1]).count()/float(testing.count())
    print('测试集error：{}'.format(test_error))

    test_label=spark.read.format('csv').option('sep',',').option('header','false').load('/home/wangsky/FBDP/test_format1.csv',schema=Schema2)
    data2=test_label.join(data,['user_id','seller_id'],'left')
    t2=data2.drop('gender').fillna(0).rdd
    test_data1=t2.map(lambda line:(line[0],line[1],LabeledPoint(line[2],[line[3:]])))
    test_data1.cache()
    test_predicted=test_data1.map(lambda x:(x[0],x[1],svm.predict(x[2].features)))
    test_predictedDF=test_predicted.toDF(Schema2)
    print(test_predictedDF.show(5))
    test_predictedDF.toPandas().to_csv("/home/wangsky/FBDP/out.csv", header=True,index=False)
    
    
    
    
    
    