# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql

from tool.tool import *

import os
basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ActivityInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")

    def addActivity(self, data_in: dict):
        base_sql = "insert into [RewardPointDB].[dbo].[Activities] ({}) values ({})"
        cur = self.db_mssql.cursor()
        sql_item = ["Status"]
        sql_values = ["1"]
        for key in data_in:
            sql_item.append(key)
            if isinstance(data_in[key], str):  # 如果是字符串 加上引号
                sql_values.append("\'" + data_in[key] + "\'")
            else:
                sql_values.append(data_in[key])
        sql = base_sql.format(','.join(sql_item), ','.join(list(map(str, sql_values))))
        cur.execute(sql)
        self.db_mssql.commit()
        err_flag = False
        return err_flag

    def getActivity(self, data_in: dict):
        title = data_in.get("title")
        pageSize = data_in.get("pageSize")
        page = data_in.get("page")
        minTop = (page - 1) * pageSize
        maxTop = page * pageSize

        if title:
            totalLength_sql = f'select COUNT([ActivitiesID]) as res from [RewardPointDB].[dbo].[Activities] as dt where dt.Status = 1 AND dt.Title LIKE \'%{title}%\''
            sql = f'select top {maxTop} * from Activities where ActivitiesID not in (select top {minTop} ActivitiesID from Activities WHERE Status = 1 AND Title LIKE \'%{title}%\') AND Status = 1 AND Title LIKE \'%{title}%\''
        else:
            totalLength_sql = f'select COUNT([ActivitiesID]) as res from [RewardPointDB].[dbo].[Activities] as dt where dt.Status = 1'
            sql = f'select top {maxTop} * from Activities where ActivitiesID not in (select top {minTop} ActivitiesID from Activities WHERE Status = 1) AND Status = 1'

        res_df = pd.read_sql(sql=sql, con=self.db_mssql)
        totalLength = pd.read_sql(sql=totalLength_sql, con=self.db_mssql).loc[0, 'res']

        return totalLength, res_df

    def getActivityById(self, data_in: dict):

        id = data_in.get("id")
        sql = "SELECT * FROM Activities WHERE ActivitiesID = %d"%(id)
        info = pd.read_sql(sql=sql, con=self.db_mssql)
        return info

    def editActivityById(self, data_in: dict):
        cur = self.db_mssql.cursor()
        res = self.getActivityById(data_in=data_in)
        print(data_in)
        valueList = []

        for key in data_in:
            if key != "id" and key != "CreationDate" and key != "CreatedBy" and key != "ActivitiesID":
                item = "%s = \'%s"%(key, data_in[key])+"\'"
                print(item)
                valueList.append(item)

        base_sql = "update Activities set {} where ActivitiesID = %d"%(data_in.get("ActivitiesID"))

        sql = base_sql.format(','.join(valueList))

        print(sql)
        cur.execute(sql)
        self.db_mssql.commit()
        return True

    def deleteActivityById(self, data_in: dict):

        base_sql = "update Activities set Status=2 where ActivitiesID = %d" % (data_in.get("ActivitiesID"))
        print(base_sql)
        cur = self.db_mssql.cursor()
        cur.execute(base_sql)
        self.db_mssql.commit()
        return True
