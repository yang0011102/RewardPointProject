# utf-8
import pymssql

from config.dbconfig import mssqldb
from tool.tool import *
import pandas as pd
# db_mssql = pymssql.connect(**mssqldb)
# temp_sql = "create table #nameCode(name varchar(50),code int, );"  # 创建一个临时表，用于存放从NC中读取的姓名-工号信息
temp_insert = "INSERT INTO #nameCode (name, code) SELECT t.name,t.code FROM (VALUES {}) AS t(name,code);"
name_df = pd.DataFrame([['1', '111'], ['1', '222'], ['1', '333'], ['1', '444']],
                       columns=('name', 'code'))
data_in={'isAccounted': 0, 'page': 0, 'pageSize': 10, 'rewardPointsType': 'A分'}
gg = name_df.loc[name_df['name']=='2',:]
print(gg)
print(len(gg.index)==0)
if gg is None:
    print(gg)
# sql_item = ['1','2','3','4','5']
# base_sql = '''SELECT TOP {0[0]} * FROM(
#                                 SELECT ROW_NUMBER() OVER (ORDER BY RewardPointsDetailID) AS RowNumber,
#                                    dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2,
#                                       dt.DepartmentLv3,dt.FunctionalDepartment,dt.Submit,dt.Name,dt.Post,
#                                          a.Name as 积分类型,{0[1]},dt.ChangeType,dt.ChangeAmount,dt.Reason,dt.Proof,
#                                             dt.ReasonType,dt.JobId,dt.AssessmentDate,dt.IsAccounted ,dt.name as Name
#                                         FROM RewardPointDB.dbo.RewardPointsDetail dt
#                                         join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID
#                                         {0[2]}
#                                 )as rowTempTable
#                                 WHERE RowNumber > {0[3]}*({0[4]}-1)'''
# kk = base_sql.format(sql_item)
# print(kk)
# sql = "1,2"
# tol=sql.split(',')
# print(tol)