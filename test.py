# utf-8
from Interface import RewardPointInterface
from config.dbconfig import *

data={"jobid":"100283"}
worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
res=worker.query_FixedPointDetail(data_in=data)
print(res)

