# utf-8
from tool.tool import *
from config.dbconfig import *
# data={'jobid': 100297, 'page': 1, 'pageSize': 10}
# import requests,json
# _r = requests.get("http://127.0.0.1:5000/get_token/jobid:100236")
# token=_r.json().get('token')
# header={"Authorization":token,'content-type': 'application/json'}
# _r = requests.post("http://127.0.0.1:5000/ppp/yyyyy", headers=header)
# print(_r.content)
# # from Interface import *
# # from config.dbconfig import *
# worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
# rrr=worker.query_RewardPointSummary(data_in=data)
# print("------\n",rrr)
db_mssql = pymssql.connect(mssqldb)
cc=getChlidType(dbcon=db_mssql)
print(cc)