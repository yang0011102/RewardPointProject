# utf-8

from Interface import RewardPointInterface as api1
from Interface_Cython import RewardPointInterface as api2
import time
from config.dbconfig import mssqldb, ncdb

# data = {'jobid': '100236'}
data = {'page': 6, "pageSize": 10}
# data = {"PointOrderID": '59'}
start_time1 = time.time()
worker1 = api1(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
print("ini cost :", time.time() - start_time1)
res1 = worker1.query_RewardPointSummary(data)
print("work time : ", time.time() - start_time1)
print(res1)

start_time2 = time.time()
worker2 = api2(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
print("ini cost :", time.time() - start_time2)
res2 = worker2.query_RewardPointSummary(data)
print("work time : ", time.time() - start_time2)
print(res2)
