# utf-8

from Interface import RewardPointInterface
from config.dbconfig import *

data = {"pageSize": 10, "page": 1}
worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
res1 = worker.query_RewardPointSummary(data_in=data)
res2 = worker.query_FixedPoints(data_in=data)
res3 = worker.query_FixedPointDetail(data_in={'jobid': "100029"})
res4 = worker.query_rewardPoint(data_in=data)
print(res1, '\n', res2, '\n', res3, '\n', res4)
