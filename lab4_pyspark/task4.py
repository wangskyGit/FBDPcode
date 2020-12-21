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
    train_data=train_data.drop('gender')
    train_data.show(10)
    train_data=train_data.dropna(how='any')
    t=train_data.rdd
    
    training,testing=t.randomSplit([0.7,0.3],seed=10)
    training=training.map(lambda line: LabeledPoint(line[2],[line[3:]]))
    testing=testing.map(lambda line: LabeledPoint(line[2],[line[3:]]))
    print(testing.take(10))
    print(training.take(10))
    training.cache()
    
    svm=SVMWithSGD.train(training,iterations=10)
    predicted=training.map(lambda x:(x.label,svm.predict(x.features)))
    print(predicted.take(20))
    error=predicted.filter(lambda lp:lp[0]!=lp[1]).count()/float(testing.count())
    print(error)
    # totalcorrect=testing.map(lambda x:1 if svm.predict(x.features)==x.label else 0).sum()
    # accuracy=float(totalcorrect)/testing.count()
    
    # predicted=testing.map(lambda lp:(float(lsvcmodel.predict(lp.features)),lp.label))
    # metrix=BinaryClassificationMetrics(predicted)

    
    
    
    
    
    
    