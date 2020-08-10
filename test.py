# utf-8
from Interface import RewardPointInterface
from config.dbconfig import *
from time import time

worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
data = {'pageSize': 10, 'page': 2}
# t0 = time()
# res1 = worker._base_query_goods(data_in=data)
# t1 = time()
# res2 = worker._base_query_FixedPoints(data_in=data)
t2 = time()
res3 = worker.query_FixedPoints(data_in=data)
t3 = time()

print("{:.2f}".format(t3 - t2), '\n', res3)
