import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
rootpath = "E:/FBDPcode/KMeans/src/output/"
new_rootpath= "E:/FBDPcode/KMeans/src/myoutput/"
for i in range(3,6):
    print(i)
    datapath=new_rootpath+'3_'+str(i)+'NewKMoutput/clusteredInstances/part-m-00000'
    d =pd.read_csv(datapath,sep="\t",header=None,names=["point","class"])
    d = d.reset_index().groupby('index').apply(lambda x:pd.DataFrame({
        'class':x['class'],'x':x['point'].str.split(",").apply(lambda n:n[0]),'y':x['point'].str.split(",").apply(lambda n:n[1])
        }))
    col=np.where(d['class']==1,'red',np.where(d['class']==2,'blue','grey'))        
    plt.scatter(d['x'].astype(float),d['y'].astype(float),c=col)
    plt.legend()
    plt.savefig('my_3_'+str(i)+'.png',dpi=300)
    plt.close()
    
""" for i in range(3,7):
    print(i)
    datapath=rootpath+'3_'+str(i)+'KMoutput/clusteredInstances/part-m-00000'
    d =pd.read_csv(datapath,sep="\t",header=None,names=["point","class"])
    d = d.reset_index().groupby('index').apply(lambda x:pd.DataFrame({
        'class':x['class'],'x':x['point'].str.split(",").apply(lambda n:n[0]),'y':x['point'].str.split(",").apply(lambda n:n[1])
        }))
    col=np.where(d['class']==d.iloc[29,0],'red',np.where(d['class']==d.iloc[61,0],'blue','grey'))        
    plt.scatter(d['x'].astype(int),d['y'].astype(int),c=col)
    plt.legend()
    plt.savefig('3_'+str(i)+'.png',dpi=300)
    plt.close()
for i in range(3,7):
    print(i)
    datapath=rootpath+'4_'+str(i)+'KMoutput/clusteredInstances/part-m-00000'
    d =pd.read_csv(datapath,sep="\t",header=None,names=["point","class"])
    d = d.reset_index().groupby('index').apply(lambda x:pd.DataFrame({
        'class':x['class'],'x':x['point'].str.split(",").apply(lambda n:n[0]),'y':x['point'].str.split(",").apply(lambda n:n[1])
        }))
    col=np.where(d['class']==d.iloc[17,0],'red',np.where(d['class']==d.iloc[51,0],'blue',np.where(d['class']==d.iloc[23,0],'grey','green')))        
    plt.scatter(d['x'].astype(int),d['y'].astype(int),c=col)
    plt.legend()
    plt.savefig('4_'+str(i)+'.png',dpi=300)
    plt.close()
print(d) """
# with open(rootpath+'3_3KMoutput/clusteredInstances/part-m-00000') as f:
#     s = f.readlines()
#     print(s)