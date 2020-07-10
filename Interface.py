# coding:utf-8
'''
接口主体
'''
import cx_Oracle
import pymssql

from config.config import DOWNLOAD_FOLDER
from tool.tool import *


def getChlidType(dbcon) -> dict:
    '''
    返回所有积分类型的所有子类型
    :param dbcon: MSSQL连接器
    :return:
    '''
    rewardPointsType_df = pd.read_sql(
        'select RewardPointsTypeID,ParentID,ChildrenID,RewardPointsTypeCode,Name from RewardPointsType where DataStatus=0',
        dbcon)
    res = {}
    for _name in rewardPointsType_df['Name'].tolist():
        _rewardPointsType_container = []
        # 遍历多叉
        childID = rewardPointsType_df.loc[
            rewardPointsType_df['Name'] == _name, 'ChildrenID'].tolist()
        while True:
            while None in childID:  # 删除空
                childID.remove(None)
            if len(childID) != 0:
                childID_list = childID[0].split(',')
                childName = rewardPointsType_df.loc[
                    rewardPointsType_df['RewardPointsTypeID'].isin(childID_list), 'Name'].values
                childID = rewardPointsType_df.loc[
                    rewardPointsType_df['RewardPointsTypeID'].isin(childID_list), 'ChildrenID'].tolist()
                _rewardPointsType_container.extend(childName)
            else:
                break
        res[_name] = _rewardPointsType_container
    return res


class RewardPointInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")

        self.rewardPointChildType = getChlidType(dbcon=self.db_mssql)
        self.rewardPointStandard = pd.read_sql(sql='''SELECT TOP (1000) [RewardPointsStandardID],[RewardPointsTypeID]
              ,[CheckItem],[PointsAmount],[ChangeCycle] FROM [RewardPointDB].[dbo].[RewardPointsStandard]''',
                                               con=self.db_mssql)  # 积分标准表
        self.HighSchool = pd.read_sql(
            "SELECT TOP (1000) [HighSchoolID],[Name],[SchoolProject] FROM [RewardPointDB].[dbo].[HighSchool]",
            con=self.db_mssql)  # 985/211工程表
        self.HighSchoolList = self.HighSchool['Name'].tolist()

    def _set_rewardPointChildType(self):
        '''
        用于刷新内存中的所有积分类型的子类型
        :return:
        '''
        self.rewardPointChildType = getChlidType(dbcon=self.db_mssql)

    def _set_rewardPointStandard(self):
        self.rewardPointStandard = pd.read_sql(sql='''SELECT TOP (1000) [RewardPointsStandardID],[RewardPointsTypeID]
                      ,[CheckItem],[PointsAmount],[ChangeCycle] FROM [RewardPointDB].[dbo].[RewardPointsStandard]''',
                                               con=self.db_mssql)  # 积分标准表

    def _set_HighSchool(self):
        self.HighSchool = pd.read_sql(
            "SELECT TOP (1000) [HighSchoolID],[Name],[SchoolProject] FROM [RewardPointDB].[dbo].[HighSchool]",
            con=self.db_mssql)  # 985/211工程表

    def _base_query_rewardPointDetail(self, data_in: dict) -> (int, pd.DataFrame):
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
                _rewardPointsType_container = self.rewardPointChildType.get(data_in.get("rewardPointsType"))
                _rewardPointsType_container.append(data_in.get("rewardPointsType"))
                rewardPointsType = "\'" + ','.join(_rewardPointsType_container) + "\'"
                query_item.append(f"a.Name in ({rewardPointsType})")
            # 判断姓名
            if not isEmpty(data_in.get("name")):
                print("姓名")
                manName = "\'" + data_in.get('name') + "\'"
                query_item.append(f"NCDB.NAME = {manName}")
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
            if not (data_in.get("page") and data_in.get("pageSize")):  # 不分页
                print("不分页")
                base_sql = "select dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2,dt.DepartmentLv3," \
                           "dt.FunctionalDepartment,NCDB.NAME as Name,dt.Submit,dt.Post,a.Name as 积分类型,{0[0]},dt.ChangeType," \
                           "dt.ChangeAmount,dt.Reason,dt.Proof,dt.ReasonType,dt.JobId,dt.AssessmentDate,dt.IsAccounted " \
                           "from [RewardPointDB].[dbo].[RewardPointsDetail] dt " \
                           "join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID " \
                           "join openquery(NC,'select name,code from bd_psndoc where enablestate =2') as NCDB on NCDB.CODE = dt.JobId " \
                           "{0[1]}"
                sql_item = [sql_item[0], query_sql]
            else:
                print("分页")
                base_sql = '''SELECT TOP {0[0]} * FROM( 
                                SELECT ROW_NUMBER() OVER (ORDER BY RewardPointsDetailID) AS RowNumber, 
                                   dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2, 
                                      dt.DepartmentLv3,dt.FunctionalDepartment,dt.Submit,NCDB.NAME as Name,dt.Post, 
                                         a.Name as 积分类型,{0[1]},dt.ChangeType,dt.ChangeAmount,dt.Reason,dt.Proof,
                                            dt.ReasonType,dt.JobId,dt.AssessmentDate,dt.IsAccounted  
                                        FROM RewardPointDB.dbo.RewardPointsDetail dt 
                                        join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID 
                                        join openquery(NC,'select name,code from bd_psndoc where enablestate =2') as NCDB on NCDB.CODE = dt.JobId 
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
            sql = "select a.Name as 积分类型,dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2,dt.DepartmentLv3, " \
                  "dt.FunctionalDepartment,NCDB.NAME as Name,dt.Submit,dt.Post,a.Name as 积分类型, " \
                  "dt.ChangeType,dt.BonusPoints,dt.MinusPoints,dt.ChangeAmount,dt.Reason,dt.Proof,dt.ReasonType," \
                  "dt.JobId,dt.AssessmentDate,dt.IsAccounted  " \
                  "from RewardPointDB.dbo.RewardPointsDetail dt " \
                  "join RewardPointDB.dbo.RewardPointsType a on dt.RewardPointsTypeID=a.RewardPointsTypeID " \
                  "join openquery(NC,'select name,code from bd_psndoc where enablestate =2') as NCDB on NCDB.CODE = dt.JobId " \
                  "where dt.DataStatus=0 and a.DataStatus=0"
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
            print("res_df", res_df)
        print(totalLength, res_df)
        return totalLength, res_df

    def _base_query_rewardPointDetail_Full(self, data_in: dict):
        # 取详情
        if data_in.get('jobid') or data_in.get('name'):
            temp_data = data_in.copy()
            errflag = temp_data.pop('page', '404')
            if errflag == '404':
                errflag = temp_data.pop('pageSize', '404')
            _, detail_df = self._base_query_rewardPointDetail(data_in=temp_data)
            summary_df = pd.DataFrame(
                columns=('工号', '姓名', '现有A分', '现有B管理积分', '固定积分', '年度管理积分', '年度累计积分', '总获得A分', '总获得B管理积分', '总累计积分'))
            if data_in.get('name'):  # 姓名转工号
                man = detail_df.loc[detail_df['Name'] == data_in.get('name'), 'JobId'].values[0]
            else:
                man = data_in.get('jobid')
            man_data = {}
            man_data['工号'] = man
            man_select = detail_df['JobId'] == man
            #  现有A分
            BonusPoints, MinusPoints = detail_df.loc[
                (detail_df['积分类型'] == 'A分') & man_select, ['BonusPoints', 'MinusPoints']].sum(axis=0)
            ChangeAmount = detail_df.loc[
                (detail_df['积分类型'] == 'A分') & (detail_df['ChangeType'] == 1) & man_select, 'ChangeAmount'].sum(
                axis=0)  # 兑换
            man_data['现有A分'] = BonusPoints - MinusPoints - ChangeAmount
            #  现有B管理积分
            manageBonusPoints, manageMinusPoints = man_data['现有B管理积分'] = detail_df.loc[
                (detail_df['积分类型'] == '管理积分') & man_select, ['BonusPoints', 'MinusPoints']].sum(axis=0)
            man_data['现有B管理积分'] = manageBonusPoints - manageMinusPoints
            #  学历,职称积分
            SchoolTittle_base_sql = '''select 
            bd_psndoc.name as 姓名,bd_psndoc.code as 工号,tectittle.name as 职称, 
            c1.name as 学历,edu.school as 学校 
            from hi_psnjob 
            join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc 
            left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc 
            join bd_defdoc c1 on edu.education = c1.pk_defdoc 
            left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc 
            left join bd_defdoc tectittle on tectittle.pk_defdoc=hi_psndoc_title.pk_techposttitle '''
            query_item = ["hi_psnjob.endflag ='N'", "hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                          "bd_psndoc.enablestate =2", "edu.lasteducation='Y'", f"bd_psndoc.code='{man}'"]
            SchoolTittle_sql = SchoolTittle_base_sql + " where " + ' and '.join(query_item)
            SchoolTittle_df = pd.read_sql(sql=SchoolTittle_sql, con=self.db_nc)
            SchoolPoints = 0
            TittlePoints = 0
            if len(SchoolTittle_df) != 0:
                SchoolPoints = self.rewardPointStandard.loc[
                    self.rewardPointStandard['CheckItem'] == SchoolTittle_df.loc[0, '学历'], 'PointsAmount'].values[0]
                if SchoolTittle_df.loc[0, '学历'] == '本科' and SchoolTittle_df.loc[0, '学校'] in self.HighSchoolList:
                    SchoolPoints += 500
                # TittlePoints = self.rewardPointStandard.loc[
                #     self.rewardPointStandard['CheckItem'] ==SchoolTittle_df.loc[0, '职称']
                # ]
            man_data['学历积分'] = SchoolPoints
            #  职务积分

            # 填充
            summary_df = summary_df.append(man_data, ignore_index=True)

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
        if data_in:  # 不为空则按照条件查询
            query_item = ["DataStatus=0"]  # 查询条件
            # 商品名称
            if data_in.get("Name"):
                goodName = "\'" + data_in.get('Name') + "\'"
                query_item.append(f"Name = {goodName}")
            # 商品编码
            if data_in.get("GoodsCode"):
                query_item.append(f"GoodsCode in {data_in.get('GoodsCode')}")
            query_sql = " where " + ' and '.join(query_item)
            # 分页
            if not (data_in.get("page") and data_in.get("pageSize")):  # 不分页
                base_sql = "select * from [RewardPointDB].[dbo].[Goods]"
                sql_item = [query_sql]
            else:
                base_sql = "select top {0[0]} * " \
                           "from (select row_number() over(order by GoodsID asc) as rownumber ,* " \
                           "from [RewardPointDB].[dbo].[Goods] {0[1]} " \
                           ") temp_row " \
                           "where rownumber > {0[2]}*({0[3]}-1)"
                sql_item = [data_in.get("pageSize"), query_sql, data_in.get("pageSize"), data_in.get("page")]
            # 拼起来
            sql = base_sql.format(sql_item)
            print("拼sql", sql)
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
            # 计算总行数
            totalLength_sql = "select count(GoodsID) as res " \
                              "from [RewardPointDB].[dbo].[Goods] "
            lensql = totalLength_sql + " where " + ' and '.join(query_item)
            print("计算总行数sql:", lensql)
            totalLength = pd.read_sql(sql=lensql, con=self.db_mssql).loc[0, 'res']
            print("计算总行数:", totalLength)
        else:
            sql = "select * from [RewardPointDB].[dbo].[Goods] where DataStatus=0"
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
            # 计算总行数
            totalLength_sql = "select count(GoodsID) as res " \
                              "from [RewardPointDB].[dbo].[Goods] "
            print("计算总行数sql:", totalLength_sql)
            totalLength = pd.read_sql(sql=totalLength_sql, con=self.db_mssql).loc[0, 'res']
            print("计算总行数:", totalLength)

        return totalLength, res_df

    def import_goods(self, data_in: dict, file_df: pd.DataFrame):
        cur = self.db_mssql.cursor()
        # 先检查GoodsCode存不存在,不存在就新建一个
        all_goodsCode = pd.read_sql(sql="select GoodsCode from Goods where DataStatus=0", con=self.db_mssql)[
            'GoodsCode'].tolist()
        insert_item = []
        base_insert_sql = "insert into RewardPointDB.dbo.Goods(GoodsCode,Name,Price,CreatedBy) values "
        exist_goodsCode = file_df['商品编码'].drop_duplicates().tolist()
        for _code in exist_goodsCode:
            if _code not in all_goodsCode:
                _item = f"({_code},'{file_df.loc[file_df['商品编码'] == _code, '商品名称'].values[0]}'," \
                        f"{file_df.loc[file_df['商品编码'] == _code, '商品单价'].values[0]},{data_in.get('Operator')})"
                insert_item.append(_item)
        if insert_item != []:
            insert_sql = base_insert_sql + ','.join(insert_item)
            print(insert_sql)
            cur.execute(insert_sql)
        # 提交
        self.db_mssql.commit()
        # 变量清空,复用,开始插入库存
        insert_item.clear()
        base_insert_sql = "insert into RewardPointDB.dbo.InventoryDetail(" \
                          "GoodsID,ChangeType,ChangeAmount,MeasureUnit,CreatedBy) values "
        all_goodsCode = pd.read_sql("select GoodsID,GoodsCode from Goods where DataStatus=0", self.db_mssql)
        for _index in file_df.index:
            _item = f"({all_goodsCode.loc[all_goodsCode['GoodsCode']==file_df.loc[_index,'商品编码'],'GoodsID'].values[0]},0," \
                    f"{file_df.loc[_index, '数量']},'{file_df.loc[_index, '商品计量单位']}',{data_in.get('Operator')})"
            insert_item.append(_item)
        insert_sql = base_insert_sql + ','.join(insert_item)
        print(insert_sql)
        cur.execute(insert_sql)
        # 提交
        self.db_mssql.commit()

    def offShelf_goods(self, data_in: dict):
        sql = f"update [RewardPointDB].[dbo].[Goods] set Status=1 where GoodsCode in ({data_in.get('GoodsCode')})"
        cur = self.db_mssql.cursor()
        cur.execute(sql)
        self.db_mssql.commit()
        return True

    def append_inventoryDetail(self, data_in: dict):
        base_sql = "insert into [RewardPointDB].[dbo].[InventoryDetail] ({}) values ({})"
        pass

    def append_goods(self, data_in: dict):
        pass
