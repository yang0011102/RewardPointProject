# utf-8
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
os.environ['path'] = 'D:\Oracle\instantclient_11_2'
from Interface import RewardPointInterface
from config.dbconfig import *
from time import time

worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
# data={"jobid":"110424"}
data={
      'pageSize':10,'page':1,
      "onduty":0
      }
t1=time()
# res=worker.query_FixedPointDetail(data_in=data)
res=worker.query_RewardPointSummary(data_in=data)
print("time costs:",f"{time()-t1:.2f}")
print(res)

