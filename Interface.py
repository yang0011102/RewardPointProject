# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql

from config.config import DOWNLOAD_FOLDER
from tool.tool import *


class RewardPointInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")

    def _base_query_rewardPointDetail(self, data_in: dict):
        '''
        用于基础积分详情查询，不直接暴露
        :param data_in:
        :return:
        '''
        if data_in:  # 不为空则按照条件查询
            print("进入条件查询")
            totalLength_sql = "select COUNT([RewardPointsdetailID]) as res " \
                              "from [RewardPointDB].[dbo].[RewardPointsDetail] as dt " \
                              "join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID "
            query_item = ["dt.DataStatus=0", "a.DataStatus=0"]  # 查询条件
            sql_item = []  # sql拼接条件
            # 工号
            if data_in.get("jobid"):
                print("判断工号")
                query_item.append(f"dt.JobId = {data_in.get('jobid')}")
            # 结算
            if not isEmpty(data_in.get("isAccounted")):
                print("结算")
                query_item.append(f"dt.IsAccounted = {data_in.get('isAccounted')}")
            # 开始时间
            if not isEmpty(data_in.get("beginDate")):
                print("开始时间")
                beginDate = "\'" + data_in.get('beginDate') + "\'"
                query_item.append(f"dt.AssessmentDate >= {beginDate}")
            # 结束时间
            if not isEmpty(data_in.get("endDate")):
                print("结束时间")
                endDate = "\'" + data_in.get('endDate') + "\'"
                query_item.append(f"dt.AssessmentDate <= {endDate}")
            # 积分类型
            if not isEmpty(data_in.get("rewardPointsType")):
                print("积分类型")
                _rewardPointsType_container=[]
                rewardPointsType_df = pd.read_sql(
                    'select RewardPointsTypeID,ParentID,ChildrenID,RewardPointsTypeCode,Name from RewardPointsType where DataStatus=0',
                    self.db_mssql)
                childID = rewardPointsType_df.loc[
                    rewardPointsType_df['Name'] == data_in.get('rewardPointsType'), 'ChildrenID']
                if len(childID.index) != 0:
                    childID_list = childID.values[0]
                rewardPointsType = "\'" + data_in.get('rewardPointsType') + "\'"
                query_item.append(f"a.Name = {rewardPointsType}")
            # 判断姓名
            if not isEmpty(data_in.get("name")):
                print("姓名")
                manName = "\'" + data_in.get('name') + "\'"
                query_item.append(f"dt.Name = {manName}")
            # 是否加分
            print("判断是否加分")
            if (not data_in.get("isBonus")) or data_in.get("isBonus") == "":
                sql_item.append("dt.BonusPoints,dt.MinusPoints")
            elif data_in.get("isBonus") == 0:
                sql_item.append("dt.BonusPoints")
                query_item.append("dt.BonusPoints > 0")
            elif data_in.get("isBonus") == 1:
                sql_item.append("dt.MinusPoints")
                query_item.append("dt.MinusPoints > 0")
            query_sql = ' where ' + ' and '.join(query_item)
            print(query_sql)
            # 分页
            if not (data_in.get("page") or data_in.get("pageSize")):  # 不分页
                print("不分页")
                base_sql = "select dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2,dt.DepartmentLv3," \
                           "dt.FunctionalDepartment,dt.Name,dt.Submit,dt.Post,a.Name as 积分类型,{0[0]},dt.ChangeType," \
                           "dt.ChangeAmount,dt.Reason,dt.Proof,dt.ReasonType,dt.JobId,dt.AssessmentDate,dt.IsAccounted " \
                           "from [RewardPointDB].[dbo].[RewardPointsDetail] dt " \
                           "join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID {0[1]}"
                sql_item = [sql_item[0], query_sql]
            else:
                print("分页")
                base_sql = '''SELECT TOP {0[0]} * FROM(
                                    SELECT ROW_NUMBER() OVER (ORDER BY RewardPointsDetailID) AS RowNumber, 
                                       dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2, 
                                          dt.DepartmentLv3,dt.FunctionalDepartment,dt.Submit,dt.Name,dt.Post, 
                                             a.Name as 积分类型,{0[1]},dt.ChangeType,dt.ChangeAmount,dt.Reason,dt.Proof,
                                                dt.ReasonType,dt.JobId,dt.AssessmentDate,dt.IsAccounted  
                                            FROM RewardPointDB.dbo.RewardPointsDetail dt 
                                            join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID 
                                            {0[2]}
                                )as rowTempTable 
                                WHERE RowNumber > {0[3]}*({0[4]}-1)'''
                sql_item = [data_in.get("pageSize"), sql_item.copy()[0],
                            query_sql, data_in.get("pageSize"), data_in.get("page")]
            sql = base_sql.format(sql_item)
            print("拼SQL:", sql)
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
            print("读SQL:\n", res_df.size)
            # 计算总行数
            lensql = totalLength_sql + " where " + ' and '.join(query_item)
            print("计算总行数sql:", lensql)
            totalLength = pd.read_sql(sql=lensql, con=self.db_mssql).loc[0, 'res']
            print("计算总行数:", totalLength)
        else:
            print("进入无条件查询")
            print("计算总行数")
            totalLength_sql = "select COUNT([RewardPointsdetailID]) as res from [RewardPointDB].[dbo].[RewardPointsDetail]"
            totalLength = pd.read_sql(sql=totalLength_sql, con=self.db_mssql).loc[0, 'res']
            print("计算总行数:", totalLength)
            sql = "select a.Name as 积分类型,dt.* " \
                  "from RewardPointDB.dbo.RewardPointsDetail dt " \
                  "join RewardPointDB.dbo.RewardPointsType a on dt.RewardPointsTypeID=a.RewardPointsTypeID " \
                  "where dt.DataStatus=0 and a.DataStatus=0"
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
            print("res_df", res_df)
        print(totalLength, res_df)
        return totalLength, res_df

    def _base_query_rewardPointSummary(self, data_in: dict):
        pass

    def query_rewardPoint(self, data_in: dict):
        '''
        返回积分详情查询信息
        :param data_in:
        :return:
        '''
        return self._base_query_rewardPointDetail(data_in=data_in)

    def delete_rewardPoint(self, data_in: dict):
        print("进入删除", data_in)
        base_sql = "update [RewardPointDB].[dbo].[RewardPointsDetail] set DataStatus=1 where RewardPointsdetailID = {}"
        sql = base_sql.format(data_in.get("RewardPointsdetailID"))
        print("执行删除sql", sql)
        cur = self.db_mssql.cursor()
        cur.execute(sql)
        print("执行删除sql")
        self.db_mssql.commit()
        print("提交操作")
        return True

    def export_rewardPoint(self, data_in: dict):
        '''
        包装积分详情信息，生成EXCEL并返回下载链接
        :param data_in:
        :return:
        '''
        _, res_df = self._base_query_rewardPointDetail(data_in=data_in)
        filename = str(data_in.get("Operator")) + str(time.time()) + ".xlsx"
        print('文件名', filename)
        filepath = DOWNLOAD_FOLDER + '/' + filename
        print('文件路径', filepath)
        res_df.to_excel(filepath, index=False)
        print('保存到', filepath)
        return "http://192.168.40.229:8080/download/" + filename  # 传回相对路径

    def import_rewardPoint(self, data_in: dict, file_df: pd.DataFrame):
        rewardPointType_df = pd.read_sql(
            "select RewardPointsTypeID,Name from RewardPointDB.dbo.RewardPointsType where DataStatus=0",
            con=self.db_mssql)
        base_sql = "insert into [RewardPointDB].[dbo].[RewardPointsDetail] ( {} ) values ( {} )"
        cur = self.db_mssql.cursor()
        column_dic = {'工号': 'JobId', '一级部门': "DepartmentLv1", '二级部门': "DepartmentLv2", '三级部门': "DepartmentLv3",
                      '职务名称': "Post", '姓名': 'Name',
                      'A分/B分': "RewardPointsTypeID",
                      '加分': "BonusPoints", '减分': "MinusPoints", '加减分理由': "Reason",
                      '加减分依据': "Proof",
                      '理由分类': "ReasonType", '职能部门/所在部门管理': "FunctionalDepartment", '提交部门': "Submit",
                      '考核日期': "AssessmentDate"}
        columns = file_df.columns.tolist()  # 取列名
        print("进入sql拼接")
        for _index in file_df.index.tolist():
            sql_item = ['ChangeType', 'CreatedBy']
            sql_values = [0, data_in.get("Operator")]
            for (k, v) in column_dic.items():
                if k in columns:
                    sql_item.append(v)
                    value = file_df.loc[_index, k]
                    if pd.isna(value):
                        print('空检查')
                        sql_values.append("\'" + '' + "\'")
                        continue

                    print('检查是否积分类型')
                    if k == 'A分/B分':  # 连RewardPointsType表查积分类型ID
                        RewardPointsTypeID = rewardPointType_df.loc[
                            rewardPointType_df['Name'] == value, 'RewardPointsTypeID'].values[0]
                        sql_values.append(RewardPointsTypeID)
                        continue
                    print("检查是否Timestamp")
                    if isinstance(value, pd.Timestamp):  # 如果是Timestamp转字符串
                        sql_values.append("\'" + datetime_string(pd.to_datetime(value, "%Y-%m-%d %H:%M:%S")) + "\'")
                        continue
                    print("检查是否字符串")
                    if isinstance(value, str):  # 如果是字符串 加上引号
                        sql_values.append("\'" + value + "\'")
                    else:
                        sql_values.append(value)
                else:
                    pass
            print("拼接sql")
            sql = base_sql.format(','.join(sql_item), ','.join(list(map(str, sql_values))))
            print(sql)
            cur.execute(sql)
            print("sql执行完毕")
        print("sql提交")
        self.db_mssql.commit()
        print("sql提交完毕")
        return True

    def account_rewardPoint(self, data_in: dict):
        print("进入结算")
        base_sql = "update [RewardPointDB].[dbo].[RewardPointsDetail] set IsAccounted=1 where {} in "
        cur = self.db_mssql.cursor()
        err_flag = False
        if not isEmpty(data_in.get("RewardPointsdetailID")):
            print("进入结算：ID")
            sql = base_sql.format("RewardPointsdetailID") + "(" + data_in.get("RewardPointsdetailID") + ")"
            cur.execute(sql)
            self.db_mssql.commit()

        elif not isEmpty(data_in.get("jobid")):
            print("进入结算:工号")
            sql = base_sql.format("JobId") + "(" + data_in.get("jobid") + ")"
            cur.execute(sql)
            self.db_mssql.commit()
        else:
            err_flag = True
        return err_flag

    def query_goods(self, data_in: dict):
        totalLength = 1
        if data_in:  # 不为空则按照条件查询
            query_item = ["DataStatus=0"]  # 查询条件
            # 分页
            if not (data_in.get("page") and data_in.get("pageSize")):  # 不分页
                base_sql = "select * from [RewardPointDB].[dbo].[Goods]"
            else:
                totalLength_sql = "select count(rownumber) row_number() over(order by Goods asc) as rownumber " \
                                  "from Goods where DataStatus=0"
                totalLength = pd.read_sql(sql=totalLength_sql, con=self.db_mssql).loc[0, 0]
                base_sql = "select top pageSize * " \
                           "from (select row_number() over(order by Goods asc) as rownumber ,* " \
                           "from Goods) temp_row  where "
                query_item.append("rownumber>((page-1)*pageSize)")
            # 商品名称
            if data_in.get("Name"):
                goodName = "\'" + data_in.get('Name') + "\'"
                query_item.append(f"Name = {goodName}")
            # 商品名称
            if data_in.get("GoodsCode"):
                query_item.append(f"GoodsCode in {data_in.get('GoodsCode')}")
            # 拼起来
            sql = base_sql + ' and '.join(query_item)
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
        else:
            sql = "select * from Goods where DataStatus=0"
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)

        return totalLength, res_df

    def import_goods(self, data_in: dict, file_df: pd.DataFrame):
        pass

    def offShelf_goods(self, data_in: dict):
        base_sql = "update Goods set Status=1 where GoodsCode in " + data_in.get("GoodsCode")
        cur = self.db_mssql.cursor()
        cur.execute(base_sql)
        cur.commit()
        return True

    def append_inventoryDetail(self, data_in: dict):
        base_sql = "insert into [RewardPointDB].[dbo].[InventoryDetail] ({}) values ({})"
        pass

    def append_goods(self, data_in: dict):
        pass
