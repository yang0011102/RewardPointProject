# utf-8
import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
os.environ['path'] = 'D:\Oracle\instantclient_11_2'
from pandas import DataFrame, read_sql
from numpy import nan, NAN, NaN
from config.dbconfig import ncdb as ncDbInfo
import cx_Oracle

con = db_nc = cx_Oracle.connect(
    f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
    encoding="UTF-8", nencoding="UTF-8")
dep_df = read_sql(
    "select dept.pk_dept, dept.pk_fatherorg, dept.name from org_dept dept where dept.depttype=0 and dept.enablestate=2",
    con=con).reindex(columns=('PK_DEPT', 'PK_FATHERORG', 'NAME', "FULLPATH")).fillna({'PK_FATHERORG': "~"})
PK_DEPT = "1001A3100000000P489Z"
exist_FullPath = dep_df.loc[(dep_df["PK_DEPT"] == PK_DEPT) & (dep_df["FULLPATH"].notnull()), "FULLPATH"]
print(exist_FullPath)
