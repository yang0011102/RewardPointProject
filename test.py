# utf-8
import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
os.environ['path'] = 'D:\Oracle\instantclient_11_2'
from Interface import RewardPointInterface
from config.dbconfig import *
from time import time

worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
# data = {"jobid": "100004"}
data = {
    'pageSize': 10, 'page': 1,"Operator":'100236'
    # "onduty": 0
}
t1 = time()
res = worker.export_rewardPoint(data_in=data)
# _,res=worker._base_query_FixedPoints(data_in=data)
print("time costs:", f"{time() - t1:.2f}")
