# utf-8
import pymssql
from Interface import *
from config.dbconfig import mssqldb,ncdb
from tool.tool import *
import pandas as pd
# db_mssql = pymssql.connect(**mssqldb)
# temp_sql = "create table #nameCode(name varchar(50),code int, );"  # 创建一个临时表，用于存放从NC中读取的姓名-工号信息
# temp_insert = "INSERT INTO #nameCode (name, code) SELECT t.name,t.code FROM (VALUES {}) AS t(name,code);"
# name_df = pd.DataFrame([['1', '111'], ['1', '222'], ['1', '333'], ['1', '444']],
#                        columns=('name', 'code'))
# data_in={'isAccounted': 0, 'page': 0, 'pageSize': 10, 'rewardPointsType': 'A分'}
# gg = name_df.loc[name_df['name']=='2',:]
# print(gg)
# print(len(gg.index)==0)
# if gg is None:
#     print(gg)
# sql_item = ['1','2','3','4','5']
file_df = pd.read_excel("./files/商品导入表.xlsx")
worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
# print(worker.import_goods(data_in={"Operator": 100236},
#                           file_df=file_df))
print(worker.export_goods(data_in={"page": 1, "pageSize": 10}))