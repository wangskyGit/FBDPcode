from __future__ import print_function

import sys
from operator import add
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext
if __name__ == "__main__":
    sc = SparkContext("local", "task2")
    d1=sc.textFile('/home/wangsky/FBDP/fulldata')
    
    buy_data=d1.filter(lambda x:x.split(',')[8]=='2' and x.split(',')[2]!='2')#所有购买的数据
    sex_ratio=buy_data.map(lambda x:(x.split(',')[0],x.split(',')[2]))\
        .reduceByKey(lambda x,y:x)\
            .map(lambda x:(x[1],x[0]))\
                .groupByKey().mapValues(len).collect()
                
    age_ratio=buy_data.map(lambda x:(x.split(',')[0],x.split(',')[1]))\
        .reduceByKey(lambda x,y:x)\
            .map(lambda x:(x[1],x[0]))\
                .groupByKey().mapValues(len).collect()
                
    age_ratio=pd.DataFrame(age_ratio,columns=['age','ratio'])
    sex_ratio=pd.DataFrame(sex_ratio,columns=['gender','ratio'])
    
    print('性别分布\n',sex_ratio)   
    sex_ratio['ratio']=sex_ratio['ratio'].apply(lambda x:x/sex_ratio['ratio'].sum())
    print('性别比例\n',sex_ratio.sort_values(by='ratio'))
    print('年龄分布\n',age_ratio)
    age_ratio['ratio']=age_ratio['ratio'].apply(lambda x:x/age_ratio['ratio'].sum())
    print('年龄比例\n',age_ratio.sort_values(by='ratio'))
    sc.stop()
  #性别分布 [('""', 6436), ('1', 121670), ('0', 285638)] 