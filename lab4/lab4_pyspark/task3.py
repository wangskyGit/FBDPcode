from __future__ import print_function

import sys
from operator import add
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext
if __name__ == "__main__":
    spark = SparkSession.builder \
    .master("local") \
    .appName("task3") \
    .config('spark.sql.crossJoin.enabled','true')\
    .getOrCreate()
    user_log=spark.read.format('csv').option('sep',',').option('header','true').load('/home/wangsky/FBDP/user_log_format1.csv')
    user_info=spark.read.format('csv').option('sep',',').option('header','true').load('/home/wangsky/FBDP/user_info_format1.csv')
    user_log.createOrReplaceTempView('user_log')
    user_info.createOrReplaceTempView('user_info')
    data1=spark.sql("select distinct user_id,gender,age_range from user_info natural join user_log \
        where action_type==\'2\' and gender!=\'2\' and age_range!=\'0\' and gender!=\'\"\"\' and age_range!=\'\"\"\'")
    data1.createOrReplaceTempView('data')
    gender=spark.sql("SELECT\
        gender,\
        number,\
        number / total percent\
        FROM\
        (\
            SELECT\
            *\
            FROM\
            (\
                SELECT\
                gender,\
                COUNT(1) number\
                FROM\
                data\
                GROUP BY\
                gender\
            ) t1\
            INNER JOIN(\
                SELECT\
                COUNT(1) total\
                FROM\
                data\
            ) t2 ON 1 = 1\
        ) t")
    gender.show()
    age=spark.sql("SELECT\
        age_range,\
        number,\
        number / total percent\
        FROM\
        (\
            SELECT\
            *\
            FROM\
            (\
                SELECT\
                age_range,\
                COUNT(1) number\
                FROM\
                data\
                GROUP BY\
                age_range\
            ) t1\
            INNER JOIN(\
                SELECT\
                COUNT(1) total\
                FROM\
                data\
            ) t2 ON 1 = 1\
        ) t")
    age.show()
    spark.stop()

    
    
