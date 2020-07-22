# utf-8
import os
os.environ['ORACLE_HOME']="/home/yy/Project/ORACLE/instantclient_11_2"
os.environ['LD_LIBRARY_PATH']="/home/yy/Project/ORACLE/instantclient_11_2"

# from Interface import *
from config.dbconfig import ncdb as ncDbInfo
import pandas as pd
import cx_Oracle
import dingtalk.client
import numpy as np
# from tool.tool import *
# db_mssql = pymssql.connect(**mssqldb)
# temp_sql = "create table #nameCode(name varchar(50),code int, );"  # 创建一个临时表，用于存放从NC中读取的姓名-工号信息
# temp_insert = "INSERT INTO #nameCode (name, code) SELECT t.name,t.code FROM (VALUES {}) AS t(name,code);"
# name_df = pd.DataFrame([['1', '111'], ['1', '222'], ['2', '333'], ['1', '444']],
#                        columns=('name', 'code'))
# ddd  = name_df.loc[name_df['name']=='1',:].reset_index()
#
# ccc=name_df.loc[name_df['name']=='2',:].reset_index()
# print(ddd.index,'\n',ccc.index)
# print(222)
# data_in={'isAccounted': 0, 'page': 0, 'pageSize': 10, 'rewardPointsType': 'A分'}
# gg = name_df.loc[name_df['name']=='2',:]
# print(gg)
# print(len(gg.index)==0)
# if gg is None:
#     print(gg)
# sql_item = ['1','2','3','4','5']
# file_df = pd.read_excel("./files/商品导入表.xlsx")
# worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
# # print(worker.import_goods(data_in={"Operator": 100236},
# #                           file_df=file_df))
# print(worker._base_query_rewardPointDetail_Full(data_in={"page": 1, "pageSize": 10}))
#
# c = {'工号': 100236,
#      '现有A分': 0,
#      '现有B管理积分': 0,
#      '学历积分': {'schoolPoints': 2000, 'education': '硕士', 'is985211': False},
#      '职称积分': 0,
#      '工龄积分': {'begindate': '2019-06-10', 'servingAge_years': 1, 'servingAge_months': 1, 'servingAgePoints': 2186},
#      '职务积分': [
#          {'begindate': '2019-06-10', 'enddate': '2019-09-29', 'islatest': False, 'jobrank': '9级', 'jobrankpoint': 0},
#          {'begindate': '2019-09-30', 'enddate': '2019-09-30', 'islatest': False, 'jobrank': '9级', 'jobrankpoint': 0},
#          {'begindate': '2019-10-01', 'enddate': '2020-01-01', 'islatest': True, 'jobrank': '9级', 'jobrankpoint': 0}]}


# print(sub_datetime(beginDate=datetime.datetime(year=2016, month=1, day=1),
#                    endDate=datetime.datetime(year=2020, month=1, day=1)))
# print(sub_datetime_Bydayone(beginDate=datetime.datetime(year=2020, month=5, day=2),
#                             endDate=datetime.datetime(year=2020, month=12, day=30)))
print(f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}')
conn=cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")
print(pd.read_sql(con=conn,sql="select * from v$version"))

