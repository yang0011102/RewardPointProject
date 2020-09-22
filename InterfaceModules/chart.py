# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql
from pandas import read_sql
from tool import *

import os
basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ChartInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")

    # 分类统计
    def count_summary(self, data_in: dict):
        # cate_type:根据什么分类，1：业务单元，2：加减分理由
        cate_type = data_in.get("cateType")
        start_time = data_in.get("startTime")
        end_time = data_in.get("endTime")
        if cate_type == 1:
            cate_text = "DepartmentLv1"
        else:
            cate_text = "ReasonType"

        if cate_type == 3:
            limit_text = "AND rpd.BonusPoints>0"
        elif cate_type == 4:
            limit_text = "AND rpd.MinusPoints>0"
        else:
            limit_text = ''

        query_sql = '''SELECT rpd.%s as groupType, COUNT(rpd.RewardPointsdetailID) AS num, SUM(rpd.BonusPoints-rpd.MinusPoints) totalScore, sum (rpd.BonusPoints) totalBonus, sum(rpd.MinusPoints) totalMinus
FROM RewardPointsDetail rpd
WHERE rpd.DataStatus = 0
AND rpd.RewardPointsTypeID = 3
AND rpd.AssessmentDate >= '%s'
AND rpd.AssessmentDate <= '%s'
%s
GROUP BY rpd.%s
ORDER BY num DESC;''' % (cate_text, start_time, end_time, limit_text, cate_text)

        res = read_sql(sql=query_sql, con=self.db_mssql)
        return df_tolist(res)

    # 每月趋势统计
    def tendency_data(self, data_in: dict):
        start_time = data_in.get("startTime")
        end_time = data_in.get("endTime")

        query_sql = '''SELECT convert(varchar(7), rpd.AssessmentDate ,120) AS month,COUNT(1) as num, SUM(rpd.BonusPoints) as totalBonous, SUM(rpd.MinusPoints) as totalMinus, SUM(rpd.BonusPoints)-SUM(rpd.MinusPoints) as totalScore
FROM RewardPointsDetail rpd
WHERE rpd.DataStatus = 0
AND rpd.RewardPointsTypeID = 3
AND rpd.AssessmentDate >= '%s'
AND rpd.AssessmentDate <= '%s'
group by convert(varchar(7), rpd.AssessmentDate ,120)
ORDER BY month;''' % (start_time, end_time)
        res = read_sql(sql=query_sql, con=self.db_mssql)
        return df_tolist(res)

    def func_reason_data(self, data_in: dict):
        start_time = data_in.get("startTime")
        end_time = data_in.get("endTime")
        query_sql = '''SELECT rpd.FunctionalDepartment,rpd.ReasonType,SUM(rpd.BonusPoints) AS totalBonous,SUM(rpd.MinusPoints) as totalMinus, SUM(rpd.BonusPoints)-SUM(rpd.MinusPoints) AS totalScore
                        FROM RewardPointsDetail rpd
                        WHERE rpd.DataStatus = 0
                        AND rpd.RewardPointsTypeID = 3
                        AND rpd.AssessmentDate >= '%s'
                        AND rpd.AssessmentDate <= '%s'
                        GROUP BY rpd.FunctionalDepartment, rpd.ReasonType
                        ORDER BY rpd.FunctionalDepartment, totalScore desc;''' % (start_time, end_time)
        res = read_sql(sql=query_sql, con=self.db_mssql)
        return df_tolist(res)

    def ranking(self, data_in: dict):
        # cate_type:根据什么分类，1：职务级别，2：公司/业务单元，3：一级部门
        cate_type = data_in.get("cateType")
        start_time = data_in.get("startTime")
        end_time = data_in.get("endTime")

        if cate_type == 1:
            select_text = ""
            cate_text = "Post"
        elif cate_type == 2:
            select_text = ""
            cate_text = "DepartmentLv1"
        else:
            select_text = 'DepartmentLv1 as lv1,'
            cate_text = "DepartmentLv1,rpd.DepartmentLv2"

        query_sql = '''SELECT %s rpd.%s as groupType,SUM(rpd.BonusPoints) totalBonus, SUM(rpd.MinusPoints) totalMinus, SUM(rpd.BonusPoints-rpd.MinusPoints) totalScore
FROM RewardPointsDetail rpd
WHERE rpd.DataStatus = 0
AND rpd.RewardPointsTypeID = 3
AND rpd.AssessmentDate >= '%s'
AND rpd.AssessmentDate <= '%s'
GROUP BY rpd.%s
ORDER BY totalScore DESC;''' % (select_text,cate_text,start_time, end_time,cate_text)
        res = read_sql(sql=query_sql, con=self.db_mssql)
        return df_tolist(res)

