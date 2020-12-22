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
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
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
    train_data.show(5)
    t=train_data.drop('gender').dropna(how='any').rdd
    
   
    re=pd.DataFrame(columns=['sample_seed','SVM_error','LR_error'])
    training_split,testing_split=t.randomSplit([0.7,0.3],seed=10)
    positive1=testing_split.filter(lambda l:l[2]==1).count()
    training=training_split.map(lambda line: LabeledPoint(line[2],[line[3:]]))
    testing=testing_split.map(lambda line: LabeledPoint(line[2],[line[3:]]))
    training.cache()
    testing.cache()
    lr=LogisticRegressionWithLBFGS()
    LR=lr.train(training,iterations=10)
    SVM=SVMWithSGD.train(training,iterations=10)
    predicted_svm=training.map(lambda x:(x.label,SVM.predict(x.features)))
    predicted_lr=training.map(lambda x:(x.label,LR.predict(x.features)))
    errorSVM=predicted_svm.filter(lambda lp:lp[0]!=lp[1]).count()/float(training.count())
    errorLR=predicted_lr.filter(lambda lp:lp[0]!=lp[1]).count()/float(training.count())
    positive=predicted_lr.filter(lambda x:x[1]==1).count()
   
    print('SVM训练集error：{}'.format(errorSVM))
    print('LR训练集error：{}'.format(errorLR))
    test_preSVM=testing.map(lambda x:(x.label,SVM.predict(x.features)))
    test_error=test_preSVM.filter(lambda lp:lp[0]!=lp[1]).count()/float(testing.count())
    test_preLR=testing.map(lambda x:(x.label,LR.predict(x.features)))
    test_errorLR=test_preLR.filter(lambda lp:lp[0]!=lp[1]).count()/float(testing.count())
    print('SVM测试集error：{}'.format(test_error))
    print('LR测试集error：{}'.format(test_errorLR))
    
    
    
    
    
    
    