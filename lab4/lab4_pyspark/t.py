import sys
from operator import add
import pandas as pd
import os
def trans_action(df):
    dd=pd.DataFrame()
    dd.loc[0,['user_id']]=df.iloc[0,0]
    dd.loc[0,['seller_id']]=df.iloc[0,5]
    dd.loc[0,['age']]=df.iloc[0,1]
    dd.loc[0,['gender']]=df.iloc[0,2]
    s=[0,0,0,0]
    for i in range(0,len(df)):
        for j in range(0,4):
            if int(df.iloc[i,8])==j:
                s[j]+=1
    dd.loc[0,['0','1','2','3']]=s
    return dd
        
if __name__ == "__main__":
    path='/home/wangsky/FBDP/fulldata/'
    for name in os.listdir(path):
        print(name)
        print("0%")
        data=pd.read_csv('/home/wangsky/FBDP/fulldata/'+name,sep=',',header=None)
        print("25%")
        data.columns=['user_id','age','gender','item_id','cat_id','seller_id','brand_id','time','action']
        newdf1=data.groupby(['user_id','seller_id'],as_index=False).apply(trans_action)
        print("50%")
        newdf2=newdf1.join(pd.get_dummies(newdf1.gender,prefix='gender'))
        print("75%")
        newdf2.to_csv('/home/wangsky/FBDP/processed_data/'+name,index=False,header=False)
        print("100%")

    