from __future__ import print_function

import sys
from operator import add
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "most popular item")
    total_data=sc.textFile('/home/wangsky/FBDP/fulldata')
    seller=total_data.filter(lambda x: x.split(',')[1]<='3' and x.split(',')[8]!='0').map(lambda x: (x.split(',')[5],1))
    item=total_data.filter(lambda x: x.split(',')[8]!='0').map(lambda x: (x.split(',')[3],1))
    newseller=seller.reduceByKey(add).sortBy(lambda x: x[1],ascending=False).take(100)
    newitem=item.reduceByKey(add).sortBy(lambda x: x[1],ascending=False).take(100)
    print(newitem)
    print('=====seller:')
    print(newseller)
    pd.DataFrame(newitem).to_csv('/home/wangsky/FBDP/pyspark_item_output',index=False)
    pd.DataFrame(newseller).to_csv('/home/wangsky/FBDP/pyspark_seller_output',index=False)
    sc.stop()