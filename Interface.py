# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql

from tool.tool import *


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

    def _base_query_RewardPointSummary(self, data_in: dict) -> (int, pd.DataFrame):
        # 基本信息
        length_base_sql = '''
select  
count(bd_psndoc.code) as res
from hi_psnjob
join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc 
join bd_defdoc c1 on edu.education = c1.pk_defdoc
left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc 
left join bd_defdoc tectittle on tectittle.pk_defdoc=hi_psndoc_title.pk_techposttitle 
where hi_psnjob.endflag = 'N' and hi_psnjob.ismainjob = 'Y' and hi_psnjob.lastflag = 'Y' and bd_psndoc.enablestate = 2
and edu.lasteducation = 'Y'
'''
        today = datetime.datetime.today()  # 今天
        newYearsDay = datetime.datetime(year=today.year, month=1, day=1)  # 今年元旦
        query_item = ["hi_psnjob.endflag ='N'", "hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                      "bd_psndoc.enablestate =2", "edu.lasteducation='Y'"]
        # temp_data = data_in.copy()
        # errflag = temp_data.pop('page', '404')
        # if errflag == '404':
        #     errflag = temp_data.pop('pageSize', '404')
        # # _, detail_df = self._base_query_rewardPointDetail(data_in=temp_data)
        if data_in.get('name'):  # 姓名
            query_item.append(f"bd_psndoc.name='{data_in.get('name')}'")
        if data_in.get('jobid'):
            query_item.append(f"bd_psndoc.name='{data_in.get('jobid')}'")
        # 分页
        if data_in.get('pageSize') and data_in.get('page'):
            maninfo_base_sql = '''
            select * 
            from (
            select  rownum as rowno,
            bd_psndoc.name as 姓名,bd_psndoc.code as 工号,
            c1.name as 学历,edu.school as 学校,tittlerank.NAME as 职称等级
            from hi_psnjob
            join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
            left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc 
            join bd_defdoc c1 on edu.education = c1.pk_defdoc
            left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc 
            left join bd_defdoc tittlerank on hi_psndoc_title.titlerank =tittlerank.pk_defdoc 
            {0[0]}
            ) temptable
            where temptable.rowno >{0[1]}*({0[2]}-1)'''
            query_item.append(f"rownum<={data_in.get('pageSize')}*{data_in.get('page')}")
            sql_item = [" where " + ' and '.join(query_item), data_in.get('pageSize'), data_in.get('page')]
        else:
            maninfo_base_sql = '''
            select  
            bd_psndoc.name as 姓名,bd_psndoc.code as 工号,
            c1.name as 学历,edu.school as 学校,tittlerank.NAME as 职称等级
            from hi_psnjob
            join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
            left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc 
            join bd_defdoc c1 on edu.education = c1.pk_defdoc
            left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc
            left join bd_defdoc tittlerank on hi_psndoc_title.titlerank =tittlerank.pk_defdoc  
            {0[0]}
            '''
            sql_item = [" where " + ' and '.join(query_item)]
        maninfo_sql = maninfo_base_sql.format(sql_item)
        print(maninfo_sql)
        maninfo_df = pd.read_sql(sql=maninfo_sql, con=self.db_nc)
        maninfo_df = maninfo_df.reindex(columns=(
            '姓名', '工号', '职称等级', '学历', '学校', '固定积分', '年度累计积分', '总累计积分'))
        # 计算数据长度
        totalLength = pd.read_sql(sql=length_base_sql, con=self.db_nc).loc[0, 'RES']
        all_id = maninfo_df['工号'].tolist()
        tempidlist = []
        for _ii in all_id:
            tempidlist.append("\'" + _ii + "\'")
        all_id_tupe = ','.join(tempidlist)
        # A 管理分表
        mssql_base_sql = '''
select dt.JobId,
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.ChangeType=0 then dt.BonusPoints else 0 end)-
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.ChangeType=0 then dt.MinusPoints else 0 end)-
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.ChangeType=1 then dt.ChangeAmount else 0 end)+
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.ChangeType=2 then dt.ChangeAmount else 0 end) as 现有A分,
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.BonusPoints else 0 end)-
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.MinusPoints else 0 end)-
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=1 then dt.ChangeAmount else 0 end)+
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=2 then dt.ChangeAmount else 0 end) as 现有管理积分,
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 and dt.AssessmentDate>'{0[0]}' then dt.BonusPoints else 0 end)-
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 and dt.AssessmentDate>'{0[0]}' then dt.MinusPoints else 0 end) as 年度管理积分,
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.ChangeType=0 then dt.BonusPoints else 0 end) as 总获得A分,
sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.BonusPoints else 0 end) as 总获得管理积分
from RewardPointDB.dbo.RewardPointsDetail dt
where {0[1]}
group by dt.JobId
        '''
        sql_item = [datetime_string(datetime.datetime(year=today.year - 1, month=12, day=31)), ]
        query_item = ['dt.DataStatus=0', f"dt.JobId in ({all_id_tupe})"]
        sql_item.append(' and '.join(query_item))
        mssql_sql = mssql_base_sql.format(sql_item)
        print('mssql:', mssql_sql)
        pointdetail = pd.read_sql(mssql_sql, self.db_mssql)
        maninfo_df = pd.merge(maninfo_df, pointdetail, left_on='工号', right_on='JobId', how='left')
        #  工龄表
        ServingAge_base_sql = '''
        select bd_psndoc.code,
        min(hi_psnjob.begindate) as begindate
        from hi_psnjob
        join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
        where
        hi_psnjob.ismainjob ='Y' 
        and bd_psndoc.enablestate =2
        and (hi_psnjob.enddate>'2018-01-01' or hi_psnjob.endflag='N')
        and bd_psndoc.code in ({})
        group by bd_psndoc.code 
                    '''
        ServingAge_sql = ServingAge_base_sql.format(all_id_tupe)
        manServing_df = pd.read_sql(ServingAge_sql, self.db_nc)

        #  职务表
        jobrank_base_sql = '''
        select 
        ncinfo.*,std.PointsAmount
        from
        openquery(NC,'
                select 
                hi_psnjob.begindate,hi_psnjob.enddate,bd_psndoc.code,
                om_jobrank.jobrankname as 职等
                from hi_psnjob
                join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
                join om_jobrank on om_jobrank.pk_jobrank = hi_psnjob.pk_jobrank
                where
                hi_psnjob.ismainjob =''Y''
                and bd_psndoc.enablestate =2
                and (hi_psnjob.enddate>''2018-01-01'' or hi_psnjob.endflag=''N'')
                and bd_psndoc.code in ({})
                order by bd_psndoc.code,hi_psnjob.begindate') as ncinfo
        join RewardPointDB.dbo.RewardPointsStandard  as std on ncinfo.职等=std.CheckItem
        where std.DataStatus=0
        '''
        tempidlist = []
        for _ii in all_id:
            tempidlist.append("\'\'" + _ii + "\'\'")
        all_id_tupe = ','.join(tempidlist)
        jobrank_sql = jobrank_base_sql.format(all_id_tupe)
        print(jobrank_sql)
        jobrank_df = pd.read_sql(jobrank_sql, self.db_mssql)
        jobrank_count_time = datetime.datetime(year=2018, month=1, day=1)

        # 填充
        for i in maninfo_df.index:
            man = maninfo_df.loc[i, '工号']
            # 学历积分
            SchoolPoints = 0
            if len(self.rewardPointStandard.loc[
                       self.rewardPointStandard['CheckItem'] == maninfo_df.loc[i, '学历'], 'PointsAmount']) == 1:
                SchoolPoints = self.rewardPointStandard.loc[
                    self.rewardPointStandard['CheckItem'] == maninfo_df.loc[0, '学历'], 'PointsAmount'].values[0]
                if maninfo_df.loc[0, '学历'] == '本科' and maninfo_df.loc[0, '学校'] in self.HighSchoolList:
                    SchoolPoints += 500
            # maninfo_df.loc[i, '学历积分'] = SchoolPoints
            # 职称积分
            TittlePoints = 0  # 这个还没做
            if len(self.rewardPointStandard.loc[
                       self.rewardPointStandard['CheckItem'] == maninfo_df.loc[i, '职称等级'], 'PointsAmount']) > 0:
                TittlePoints = self.rewardPointStandard.loc[
                    self.rewardPointStandard['CheckItem'] == maninfo_df.loc[i, '职称等级'], 'PointsAmount'].values[0]
            # maninfo_df.loc[i, '职称积分'] = TittlePoints
            # 工龄积分
            serving_begindate = datetime.datetime.strptime(
                manServing_df.loc[manServing_df['CODE'] == man, 'BEGINDATE'].values[0], "%Y-%m-%d")  # 取起始时间
            serving_count_time = datetime.datetime(year=2014, month=1, day=1)
            if serving_begindate.__le__(serving_count_time):  # 如果早于2004-01-01,那么从2004-01-01开始算
                serving_begindate = serving_count_time
            years, months = sub_datetime_Bydayone(beginDate=serving_begindate,
                                                  endDate=datetime.datetime(year=today.year, month=12, day=30))
            ServingAgePoints = int((years + months / 12) * 2000)
            # maninfo_df.loc[i, '工龄积分'] = ServingAgePoints
            # 职务积分
            jobrankpoint = 0  #
            man_workinfo = jobrank_df.loc[jobrank_df['CODE'] == man, :].reset_index()  # 取个人的工作信息
            if len(man_workinfo) > 0:
                jobrank_begindate = datetime.datetime.strptime(man_workinfo.loc[0, 'BEGINDATE'], "%Y-%m-%d")  # 取起始时间
                if jobrank_begindate.__le__(jobrank_count_time):  # 如果早于2018-01-01,那么从2018-01-01开始算
                    jobrank_begindate = jobrank_count_time
                man_workinfo.loc[0, 'BEGINDATE'] = jobrank_begindate  # 填回去
                for _index in man_workinfo.index:
                    if pd.isna(man_workinfo.loc[_index, 'ENDDATE']):
                        temp_enddate = today
                    else:
                        temp_enddate = datetime.datetime.strptime(man_workinfo.loc[_index, 'ENDDATE'], "%Y-%m-%d")
                    if isinstance(man_workinfo.loc[_index, 'BEGINDATE'], str):
                        temp_begindate = datetime.datetime.strptime(man_workinfo.loc[_index, 'BEGINDATE'], "%Y-%m-%d")
                    else:
                        temp_begindate = man_workinfo.loc[_index, 'BEGINDATE']
                    if temp_begindate.__ge__(newYearsDay):  # 过了元旦不算
                        continue
                    elif temp_enddate.__ge__(newYearsDay):
                        temp_enddate = datetime.datetime(year=today.year - 1, month=12, day=31)
                    years, months = sub_datetime_Bydayone(beginDate=temp_begindate, endDate=temp_enddate)
                    temp_standard = 0
                    if len(man_workinfo.loc[_index, '职等']) == 1:
                        temp_standard = man_workinfo.loc[_index, '职等']
                    jobrankpoint += int(temp_standard * (months + years * 12))
            # maninfo_df.loc[i, '职务积分'] = jobrankpoint
            maninfo_df.loc[i, '固定积分'] = SchoolPoints + TittlePoints + ServingAgePoints + jobrankpoint
            # 年度固定积分
            year_SchoolPoints, year_TittlePoints, year_ServingAgePoints, year_jobrankpoint = 0, 0, 0, 0
            if serving_begindate.year >= today.year:
                year_SchoolPoints = SchoolPoints
                year_ServingAgePoints = ServingAgePoints
            else:
                year_ServingAgePoints = 2000
            maninfo_df.loc[i, '总累计积分'] = maninfo_df.loc[i, '固定积分'] + maninfo_df.loc[i, '总获得管理积分']
            maninfo_df.loc[
                i, '年度累计积分'] = year_SchoolPoints + year_TittlePoints + year_ServingAgePoints + year_jobrankpoint + \
                               maninfo_df.loc[i, '总获得管理积分']
        return totalLength, maninfo_df

    def query_RewardPointSummary(self, data_in: dict) -> (int, pd.DataFrame):
        return self._base_query_RewardPointSummary(data_in)

    def export_RewardPointSummary(self, data_in: dict):
        _, res_df = self._base_query_RewardPointSummary(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=str(data_in.get("Operator")))

    def query_B_rewardPointDetail(self, data_in: dict) -> dict:
        today = datetime.datetime.today()  # 今天
        newYearsDay = datetime.datetime(year=today.year, month=1, day=1)  # 今年元旦
        man_data = {}
        man = data_in.get('jobid')
        man_data['工号'] = man
        #  学历,职称积分
        maninfo_base_sql = '''
        select 
        bd_psndoc.name as 姓名,bd_psndoc.code as 工号,tectittle.name as 职称,tittlerank.name as 职称等级,
        c1.name as 学历,edu.school as 学校
        from hi_psnjob
        join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
        left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc 
        join bd_defdoc c1 on edu.education = c1.pk_defdoc
        left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc 
        left join bd_defdoc tectittle on hi_psndoc_title.pk_techposttitle = tectittle.pk_defdoc
        left join bd_defdoc tittlerank on hi_psndoc_title.titlerank = tittlerank.pk_defdoc 
                                         '''
        query_item = ["hi_psnjob.endflag ='N'", "hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                      "bd_psndoc.enablestate =2", "edu.lasteducation='Y'", f"bd_psndoc.code='{man}'"]
        maninfo_sql = maninfo_base_sql + " where " + ' and '.join(query_item)
        print(maninfo_sql)
        maninfo_df = pd.read_sql(sql=maninfo_sql, con=self.db_nc)
        SchoolPoints = 0
        is985211 = False
        TittlePoints = 0  # 这个还没做
        School_data, Tittle_data = {}, {}
        if len(maninfo_df) != 0:
            if len(self.rewardPointStandard.loc[
                       self.rewardPointStandard['CheckItem'] == maninfo_df.loc[0, '学历'], 'PointsAmount']) == 1:
                SchoolPoints = self.rewardPointStandard.loc[
                    self.rewardPointStandard['CheckItem'] == maninfo_df.loc[0, '学历'], 'PointsAmount'].values[0]
                if maninfo_df.loc[0, '学历'] == '本科' and maninfo_df.loc[0, '学校'] in self.HighSchoolList:
                    is985211 = True
                    SchoolPoints += 500
            School_data['schoolPoints'] = SchoolPoints
            School_data['education'] = maninfo_df.loc[0, '学历']
            School_data['is985211'] = is985211
            if len(self.rewardPointStandard.loc[
                       self.rewardPointStandard['CheckItem'] == maninfo_df.loc[0, '职称等级'], 'PointsAmount']) > 0:
                TittlePoints = self.rewardPointStandard.loc[
                    self.rewardPointStandard['CheckItem'] == maninfo_df.loc[0, '职称等级'], 'PointsAmount'].values[0]
            Tittle_data['tectittle'] = maninfo_df.loc[0, '职称']
            Tittle_data['tittleRank'] = maninfo_df.loc[0, '职称等级']
            Tittle_data['tittleRankPoint'] = TittlePoints
        ServingAge_base_sql = '''
        select 
        hi_psnjob.begindate,hi_psnjob.enddate
        from hi_psnjob
        join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
        where
        hi_psnjob.ismainjob ='Y' 
        and bd_psndoc.enablestate =2
        and (hi_psnjob.enddate>'2004-01-01' or hi_psnjob.endflag='N')
        and bd_psndoc.code='{}'
        order by bd_psndoc.code,hi_psnjob.begindate
                    '''
        ServingAge_sql = ServingAge_base_sql.format(man)
        manServing_df = pd.read_sql(ServingAge_sql, self.db_nc)
        #  工龄积分
        serving_data = {}
        serving_begindate = datetime.datetime.strptime(manServing_df.loc[0, 'BEGINDATE'], "%Y-%m-%d")  # 取起始时间
        serving_count_time = datetime.datetime(year=2014, month=1, day=1)
        if serving_begindate.__le__(serving_count_time):  # 如果早于2004-01-01,那么从2004-01-01开始算
            serving_begindate = serving_count_time
        years, months = sub_datetime_Bydayone(beginDate=serving_begindate,
                                              endDate=datetime.datetime(year=today.year, month=12, day=30))
        ServingAgePoints = int((years + months / 12) * 2000)
        serving_data['begindate'] = manServing_df.loc[0, 'BEGINDATE']
        serving_data['servingAge_years'] = years
        serving_data['servingAge_months'] = months
        serving_data['servingAgePoints'] = ServingAgePoints
        #  职务积分
        jobrank_data = []  # 职务积分详情容器
        jobrank_base_sql = '''
        select 
        hi_psnjob.begindate,hi_psnjob.enddate,
        om_jobrank.jobrankname as 职等
        from hi_psnjob
        join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
        join om_jobrank on om_jobrank.pk_jobrank = hi_psnjob.pk_jobrank
        where
        hi_psnjob.ismainjob ='Y' 
        and bd_psndoc.enablestate =2
        and (hi_psnjob.enddate>'2018-01-01' or hi_psnjob.endflag='N')
        and bd_psndoc.code='{}'
        order by bd_psndoc.code,hi_psnjob.begindate
        '''
        jobrank_sql = jobrank_base_sql.format(man)
        print(jobrank_sql)
        jobrank_df = pd.read_sql(jobrank_sql, self.db_nc)
        jobrank_count_time = datetime.datetime(year=2018, month=1, day=1)
        jobrank_begindate = datetime.datetime.strptime(jobrank_df.loc[0, 'BEGINDATE'], "%Y-%m-%d")  # 取起始时间
        if jobrank_begindate.__le__(jobrank_count_time):  # 如果早于2018-01-01,那么从2018-01-01开始算
            jobrank_begindate = jobrank_count_time
        jobrank_df.loc[0, 'BEGINDATE'] = jobrank_begindate  # 填回去

        for _index in jobrank_df.index:
            if pd.isna(jobrank_df.loc[_index, 'ENDDATE']):
                temp_enddate = today
                islatest = True
            else:
                temp_enddate = datetime.datetime.strptime(jobrank_df.loc[_index, 'ENDDATE'], "%Y-%m-%d")
                islatest = False
            if isinstance(jobrank_df.loc[_index, 'BEGINDATE'], str):
                temp_begindate = datetime.datetime.strptime(jobrank_df.loc[_index, 'BEGINDATE'], "%Y-%m-%d")
            else:
                temp_begindate = jobrank_df.loc[_index, 'BEGINDATE']
            if temp_begindate.__ge__(newYearsDay):  # 过了元旦不算
                continue
            elif temp_enddate.__ge__(newYearsDay):
                temp_enddate = datetime.datetime(year=today.year - 1, month=12, day=31)
            years, months = sub_datetime_Bydayone(beginDate=temp_begindate, endDate=temp_enddate)
            temp_jobrankpoint = 0
            jobrank = jobrank_df.loc[_index, '职等']
            if len(jobrank) == 1:
                temp_standard = 0
                if len(self.rewardPointStandard[
                           self.rewardPointStandard['CheckItem'] == jobrank, 'PointsAmount']) == 1:
                    temp_standard = self.rewardPointStandard[
                        self.rewardPointStandard['CheckItem'] == jobrank_df.loc[
                            _index, '职等'], 'PointsAmount'].values[0]
                temp_jobrankpoint = int(temp_standard * (months + years * 12))
            jobrank_data.append({'begindate': datetime_string(temp_begindate, timeType="%Y-%m-%d"),
                                 'enddate': datetime_string(temp_enddate, timeType="%Y-%m-%d"),
                                 'islatest': islatest,
                                 'jobrank': jobrank,
                                 'jobrankpoint': temp_jobrankpoint})

        # 存起来
        man_data['学历积分'] = School_data
        man_data['职称积分'] = Tittle_data
        man_data['工龄积分'] = serving_data
        man_data['职务积分'] = jobrank_data

        return man_data

    def query_rewardPoint(self, data_in: dict) -> (int, pd.DataFrame):
        '''
        返回积分详情查询信息
        :param data_in:
        :return:
        '''
        return self._base_query_rewardPointDetail(data_in=data_in)

    def delete_rewardPoint(self, data_in: dict) -> bool:
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

    def export_rewardPoint(self, data_in: dict) -> str:
        '''
        包装积分详情信息，生成EXCEL并返回下载链接
        :param data_in:
        :return:
        '''
        _, res_df = self._base_query_rewardPointDetail(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=str(data_in.get("Operator")))

    def import_rewardPoint(self, data_in: dict, file_df: pd.DataFrame) -> bool:
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

    def account_rewardPoint(self, data_in: dict) -> bool:
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

    def _base_query_goods(self, data_in: dict) -> (int, pd.DataFrame):
        if data_in:  # 不为空则按照条件查询
            query_item = ["DataStatus=0"]  # 查询条件
            # 商品名称
            if data_in.get("Name"):
                goodName = "\'" + data_in.get('Name') + "\'"
                query_item.append(f"goods.Name = {goodName}")
            # 商品编码
            if data_in.get("GoodsCode"):
                query_item.append(f"goods.GoodsCode in {data_in.get('GoodsCode')}")
            query_sql = " where " + ' and '.join(query_item)
            # 分页
            if not (data_in.get("page") and data_in.get("pageSize")):  # 不分页
                base_sql = '''
                select goods.GoodsCode,goods.Name,goods.PictureUrl,goods.PointCost,goods.Status,
                InOutTable.出库总量,InOutTable.入库总量,LockTable.锁定总量
                from dbo.Goods goods
                join (
                    select goods.GoodsCode,goods.Name,goods.PictureUrl,goods.PointCost,
                    goods.Status,InOutTable.出库总量,InOutTable.入库总量,LockTable.锁定总量
                    from dbo.Goods goods
                    left join (
                            select
                            goods.GoodsID,sum(stkin.ChangeAmount) as 入库总量, sum(stkout.ChangeAmount) as 出库总量
                            from dbo.Goods goods
                            left join (select ChangeAmount,GoodsID from dbo.StockInDetail where DataStatus=0 and ChangeType=0) stkin on stkin.GoodsID = goods.GoodsID
                            left join (select ChangeAmount,GoodsID from dbo.StockOutDetail where DataStatus=0 and ChangeType=0) stkout on stkout.GoodsID = goods.GoodsID
                            where 
                            goods.DataStatus=0
                            group by goods.GoodsID
                            ) as InOutTable on InOutTable.GoodsID = goods.GoodsID
                    left join (
                            select
                            goods.GoodsID,sum(stkout.ChangeAmount) as 锁定总量
                            from dbo.Goods goods
                            join (select ChangeAmount,GoodsID from dbo.StockOutDetail where DataStatus=0 and ChangeType=0) stkout on stkout.GoodsID = goods.GoodsID
                            where 
                            goods.DataStatus=0
                            group by goods.GoodsID) as LockTable on LockTable.GoodsID = goods.GoodsID  
                            where {0[0]}'''
                sql_item = [query_sql]
            else:
                base_sql = '''
                select top {0[0]} * from (
                    select row_number() over(order by goods.GoodsID asc) as rownumber, goods.GoodsCode,
                    goods.Name,goods.PictureUrl,goods.PointCost,goods.Status,InOutTable.出库总量,InOutTable.入库总量,LockTable.锁定总量
                    from dbo.Goods goods
                    left join (
                            select
                            goods.GoodsID,sum(stkin.ChangeAmount) as 入库总量, sum(stkout.ChangeAmount) as 出库总量
                            from dbo.Goods goods
                            left join (select ChangeAmount,GoodsID from dbo.StockInDetail where DataStatus=0 and ChangeType=0) stkin on stkin.GoodsID = goods.GoodsID
                            left join (select ChangeAmount,GoodsID from dbo.StockOutDetail where DataStatus=0 and ChangeType=0) stkout on stkout.GoodsID = goods.GoodsID
                            where 
                            goods.DataStatus=0
                            group by goods.GoodsID
                            ) as InOutTable on InOutTable.GoodsID = goods.GoodsID
                    left join (
                            select
                            goods.GoodsID,sum(stkout.ChangeAmount) as 锁定总量
                            from dbo.Goods goods
                            join (select ChangeAmount,GoodsID from dbo.StockOutDetail where DataStatus=0 and ChangeType=0) stkout on stkout.GoodsID = goods.GoodsID
                            where 
                            goods.DataStatus=0
                            group by goods.GoodsID
                            ) as LockTable on LockTable.GoodsID = goods.GoodsID {0[1]}
                ) temp_row where rownumber > {0[2]}*({0[3]}-1)
                '''
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

    def query_goods(self, data_in: dict) -> (int, pd.DataFrame):
        return self._base_query_goods(data_in=data_in)

    def export_goods(self, data_in: dict) -> str:
        _, res_df = self._base_query_goods(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=str(data_in.get("Operator")))

    def import_goods(self, data_in: dict, file_df: pd.DataFrame) -> bool:
        cur = self.db_mssql.cursor()
        # 先检查GoodsCode存不存在,不存在就新建一个
        all_goodsCode = pd.read_sql(sql="select GoodsCode from Goods where DataStatus=0", con=self.db_mssql)[
            'GoodsCode'].tolist()
        insert_item = []
        base_insert_sql = "insert into RewardPointDB.dbo.Goods(GoodsCode,Name,PointCost,MarketPrice,PurchasePrice,CreatedBy) values "
        exist_goodsCode = file_df['商品编码'].drop_duplicates().tolist()
        for _code in exist_goodsCode:
            if _code not in all_goodsCode:
                _item = f'''
({_code},'{file_df.loc[file_df['商品编码'] == _code, '商品名称'].values[0]}',
{file_df.loc[file_df['商品编码'] == _code, '商品单价'].values[0]},'{file_df.loc[file_df['商品编码'] == _code, '市场价'].values[0]}',
'{file_df.loc[file_df['商品编码'] == _code, '购买价'].values[0]}',{data_in.get('Operator')})'''
                insert_item.append(_item)
        if insert_item != []:
            insert_sql = base_insert_sql + ','.join(insert_item)
            print(insert_sql)
            cur.execute(insert_sql)
        # 提交
        self.db_mssql.commit()
        # 变量清空,复用,开始插入库存
        insert_item.clear()
        base_insert_sql = "insert into RewardPointDB.dbo.StockInDetail(" \
                          "GoodsID,ChangeType,ChangeAmount,MeasureUnit,CreatedBy) values "
        all_goodsCode = pd.read_sql("select GoodsID,GoodsCode from Goods where DataStatus=0", self.db_mssql)
        for _index in file_df.index:
            _item = f'''
({all_goodsCode.loc[all_goodsCode['GoodsCode'] == file_df.loc[_index, '商品编码'], 'GoodsID'].values[0]},0,
{file_df.loc[_index, '数量']},'{file_df.loc[_index, '商品计量单位']}',{data_in.get('Operator')})'''
            insert_item.append(_item)
        insert_sql = base_insert_sql + ','.join(insert_item)
        print(insert_sql)
        cur.execute(insert_sql)
        # 提交
        self.db_mssql.commit()
        return True

    def set_goods_status(self, data_in: dict) -> bool:
        sql = f"update [RewardPointDB].[dbo].[Goods] set Status={data_in.get('Status')} where GoodsCode in ({data_in.get('GoodsCode')})"
        cur = self.db_mssql.cursor()
        cur.execute(sql)
        self.db_mssql.commit()
        return True

    def upload_goodsImage(self, data_in: dict, image_url: str) -> bool:
        cur = self.db_mssql.cursor()
        base_sql = "update RewardPointDB.dbo.Goods set PictureUrl={}"
        query_item = []
        query_item.append(f"GoodsCode = {data_in.get('GoodsCode')}")
        sql = base_sql.format("\'" + image_url + "\'") + " where " + ' and '.join(query_item)
        cur.execute(sql)
        self.db_mssql.commit()
        return True

    def test1(self):
        begindate = datetime.datetime.strptime("2016-09-26", "%Y-%m-%d")
        enddate = datetime.datetime.strptime("2017-12-31", "%Y-%m-%d")
        level = "4级"
        totalmonth = (enddate.year - begindate.year) * 12 + (enddate.month - begindate.month)
        print(totalmonth)
        # date_list = []
        # for month in range(begindate.month, enddate.month + 1):
        #     date_list.append(datetime.date(year=today.year - 1, month=month, day=1))
        # pass


if __name__ == "__main__":
    from config.dbconfig import mssqldb, ncdb

    worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
    # res=worker.query_B_rewardPointDetail(data_in={"jobid": 100236})
    f,res=worker.query_RewardPointSummary(data_in={"page": 1, "pageSize": 10})
    print(1)
