# utf-8
from config.dbconfig import mssqldb, ncdb
from Interface import RewardPointInterface
import pandas as pd

worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
data = {'PointOrderID': '29'}
_response = {"code": 0,
             "msg": "",
             "data": worker.query_orderDetail(data_in=data)}
import simplejson
from tool.tool import SuperEncoder
simplejson.dumps(_response, cls=SuperEncoder,ignore_nan=True)