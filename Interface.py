# coding:utf-8
'''
接口主体
'''

import cx_Oracle
from tool.tool import *


class RewardPointInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")
        self.rewardPointChildType = getChlidType(dbcon=self.db_mssql)
        self.rewardPointStandard = pd.read_sql(sql='''SELECT [RewardPointsStandardID],[RewardPointsTypeID]
              ,[CheckItem],[PointsAmount],[ChangeCycle] FROM [RewardPointDB].[dbo].[RewardPointsStandard]''',
                                               con=self.db_mssql)  # 积分标准表
        self.HighSchool = pd.read_sql(
            "SELECT [HighSchoolID],[Name],[SchoolProject] FROM [RewardPointDB].[dbo].[HighSchool]",
            con=self.db_mssql)  # 985/211工程表
        self.HighSchoolList = self.HighSchool['Name'].tolist()
        self.jobrank_count_time = datetime.datetime(year=2018, month=8, day=1)
        self.jobrank_begin_time = datetime.datetime(year=2018, month=1, day=1)
        self.serving_count_time = datetime.datetime(year=2004, month=1, day=1)

    def _set_rewardPointChildType(self):
        '''
        用于刷新内存中的所有积分类型的子类型
        :return:
        '''
        self.rewardPointChildType = getChlidType(dbcon=self.db_mssql)

    def _set_rewardPointStandard(self):
        self.rewardPointStandard = pd.read_sql(sql='''SELECT [RewardPointsStandardID],[RewardPointsTypeID]
                      ,[CheckItem],[PointsAmount],[ChangeCycle] FROM [RewardPointDB].[dbo].[RewardPointsStandard]''',
                                               con=self.db_mssql)  # 积分标准表

    def _set_HighSchool(self):
        self.HighSchool = pd.read_sql(
            "SELECT TOP [HighSchoolID],[Name],[SchoolProject] FROM [RewardPointDB].[dbo].[HighSchool]",
            con=self.db_mssql)  # 985/211工程表

    def _base_query_rewardPointDetail(self, data_in: dict) -> (int, pd.DataFrame):
        '''
        用于基础积分详情查询，不直接暴露
        :param data_in:
        :return:
        '''
        if data_in:  # 不为空则按照条件查询
            totalLength_sql = "select COUNT([RewardPointsdetailID]) as res " \
                              "from [RewardPointDB].[dbo].[RewardPointsDetail] as dt " \
                              "join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID " \
                              "inner hash join openquery(NC,'select name,code from bd_psndoc where enablestate =2') as NCDB on NCDB.CODE = dt.JobId "
            query_item = ["dt.DataStatus=0", "a.DataStatus=0"]  # 查询条件
            sql_item = []  # sql拼接条件
            # 工号
            if data_in.get("jobid"):
                query_item.append(f"dt.JobId = {data_in.get('jobid')}")
            # 结算
            if not isEmpty(data_in.get("isAccounted")):
                query_item.append(f"dt.IsAccounted = {data_in.get('isAccounted')}")
            # 开始时间
            if not isEmpty(data_in.get("beginDate")):
                beginDate = "\'" + data_in.get('beginDate') + "\'"
                query_item.append(f"dt.AssessmentDate >= {beginDate}")
            # 结束时间
            if not isEmpty(data_in.get("endDate")):
                endDate = "\'" + data_in.get('endDate') + "\'"
                query_item.append(f"dt.AssessmentDate <= {endDate}")
            # 积分类型
            if not isEmpty(data_in.get("rewardPointsType")):
                _rewardPointsType_container = self.rewardPointChildType.get(data_in.get("rewardPointsType"))
                _rewardPointsType_container.append(data_in.get("rewardPointsType"))
                rewardPointsType = "\'" + '\',\''.join(_rewardPointsType_container) + "\'"
                query_item.append(f"a.Name in ({rewardPointsType})")
            # 判断姓名
            if not isEmpty(data_in.get("name")):
                manName = "\'" + data_in.get('name') + "\'"
                query_item.append(f"NCDB.NAME = {manName}")
            # 是否加分
            if (not data_in.get("isBonus")) or data_in.get("isBonus") == "":
                sql_item.append("dt.BonusPoints,dt.MinusPoints")
            elif data_in.get("isBonus") == 0:
                sql_item.append("dt.BonusPoints")
                query_item.append("dt.BonusPoints > 0")
            elif data_in.get("isBonus") == 1:
                sql_item.append("dt.MinusPoints")
                query_item.append("dt.MinusPoints > 0")
            query_sql = ' where ' + ' and '.join(query_item)
            base_sql = "select dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2,dt.DepartmentLv3," \
                       "dt.FunctionalDepartment,NCDB.NAME as Name,dt.Submit,dt.Post,a.Name as 积分类型,{0[0]},dt.ChangeType," \
                       "dt.ChangeAmount,dt.Reason,dt.Proof,dt.ReasonType,dt.JobId,dt.AssessmentDate,dt.IsAccounted " \
                       "from [RewardPointDB].[dbo].[RewardPointsDetail] dt " \
                       "join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID " \
                       "inner hash join openquery(NC,'select name,code from bd_psndoc where enablestate =2') as NCDB on NCDB.CODE = dt.JobId " \
                       "{0[1]} order by dt.RewardPointsdetailID desc {0[2]}"
            # 分页
            sql_item = [sql_item[0], query_sql]
            if not (data_in.get("page") and data_in.get("pageSize")):  # 不分页
                sql_item.append('')
            else:
                page_sql = " offset {0[0]}*({0[1]}-1) rows fetch next {0[0]} rows only".format(
                    [data_in.get("pageSize"), data_in.get("page")])
                sql_item.append(page_sql)
            sql = base_sql.format(sql_item)
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
            # 计算总行数
            lensql = totalLength_sql + " where " + ' and '.join(query_item)
            totalLength = pd.read_sql(sql=lensql, con=self.db_mssql).loc[0, 'res']
        else:
            totalLength_sql = "select COUNT([RewardPointsdetailID]) as res from [RewardPointDB].[dbo].[RewardPointsDetail]"
            totalLength = pd.read_sql(sql=totalLength_sql, con=self.db_mssql).loc[0, 'res']
            sql = "select a.Name as 积分类型,dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2,dt.DepartmentLv3, " \
                  "dt.FunctionalDepartment,NCDB.NAME as Name,dt.Submit,dt.Post,a.Name as 积分类型, " \
                  "dt.ChangeType,dt.BonusPoints,dt.MinusPoints,dt.ChangeAmount,dt.Reason,dt.Proof,dt.ReasonType," \
                  "dt.JobId,dt.AssessmentDate,dt.IsAccounted  " \
                  "from RewardPointDB.dbo.RewardPointsDetail dt " \
                  "join RewardPointDB.dbo.RewardPointsType a on dt.RewardPointsTypeID=a.RewardPointsTypeID " \
                  "inner hash join openquery(NC,'select name,code from bd_psndoc where enablestate =2') as NCDB on NCDB.CODE = dt.JobId " \
                  "where dt.DataStatus=0 and a.DataStatus=0"
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
        return totalLength, res_df

    def _base_query_RewardPointSummary(self, data_in: dict) -> (int, pd.DataFrame):
        # 基本信息
        length_base_sql = "select count(rownum) as res from hi_psnjob left join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc left join bd_psncl on bd_psncl.pk_psncl=hi_psnjob. pk_psncl {0[0]}"
        today = datetime.datetime.today()  # 今天
        query_item = ["hi_psnjob.endflag ='N'", "hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                      "bd_psndoc.enablestate =2", "hi_psnjob. poststat='Y'", "bd_psncl.name in ('全职','退休返聘','试用期员工')"]
        if data_in.get('name'):  # 姓名
            query_item.append(f"bd_psndoc.name='{data_in.get('name')}'")
        if data_in.get('jobid'):
            query_item.append(f"bd_psndoc.code='{data_in.get('jobid')}'")
        # 分页
        if data_in.get('pageSize') and data_in.get('page'):
            maninfo_base_sql = '''
            select * 
            from (
            select  rownum as rowno,
            bd_psndoc.name as 姓名,bd_psndoc.code as 工号,org_adminorg.name as 组织,org_dept.name as 部门
            from hi_psnjob
            join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
            left join org_dept on org_dept.pk_dept =hi_psnjob.pk_dept
            left join org_adminorg on org_adminorg.pk_adminorg  =hi_psnjob.pk_org
            left join bd_psncl on bd_psncl.pk_psncl=hi_psnjob. pk_psncl
            {0[0]}
            ) temptable
            where temptable.rowno >{0[1]}*({0[2]}-1) and temptable.rowno <={0[1]}*{0[2]}'''
            sql_item = [" where " + ' and '.join(query_item), data_in.get('pageSize'), data_in.get('page')]
        else:
            maninfo_base_sql = '''
            select  
            bd_psndoc.name as 姓名,bd_psndoc.code as 工号,org_dept.name as 部门,org_adminorg.name as 组织
            from hi_psnjob
            join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
            left join org_dept on org_dept.pk_dept =hi_psnjob.pk_dept
            left join org_adminorg on org_adminorg.pk_adminorg  =hi_psnjob.pk_org
            left join bd_psncl on bd_psncl.pk_psncl=hi_psnjob. pk_psncl
            {0[0]}
            '''
            sql_item = [" where " + ' and '.join(query_item)]
        maninfo_sql = maninfo_base_sql.format(sql_item)
        maninfo_df = pd.read_sql(sql=maninfo_sql, con=self.db_nc)
        maninfo_df = maninfo_df.reindex(columns=(
            '姓名', '工号', '组织', '部门', '职称等级', '学历', '学校', '固定积分', '年度累计积分', '总累计积分'))
        # 计算数据长度
        totalLength = pd.read_sql(sql=length_base_sql.format(sql_item), con=self.db_nc).loc[0, 'RES']
        all_id = maninfo_df['工号'].tolist()
        if len(all_id) == 0:
            return 0, maninfo_df
        tempidlist = []
        for _ii in all_id:
            tempidlist.append("\'" + _ii + "\'")
        all_id_tupe = ','.join(tempidlist)
        #  学历
        base_sql = "select ncinfo.jobid,ncinfo.educationname,ncinfo.schoolname,std.PointsAmount from openquery(NC,'select bd_psndoc.code as jobid,c1.name as educationname,edu.school as schoolname from bd_psndoc left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc left join bd_defdoc c1 on edu.education = c1.pk_defdoc where bd_psndoc.enablestate =2 and edu.lasteducation=''Y''') as ncinfo left hash join RewardPointDB.dbo.RewardPointsStandard as std on std.CheckItem=ncinfo.educationname"
        query_item = ["(std.RewardPointsTypeID=7 or std.RewardPointsTypeID is null)",
                      f"ncinfo.jobid in ({all_id_tupe})"]
        school_df = pd.read_sql(sql=base_sql + " where " + ' and '.join(query_item), con=self.db_mssql)
        #  职称
        base_sql = "select ncinfo.jobid,ncinfo.tectittlename,ncinfo.tectittlerank,std.PointsAmount from openquery(NC,'select bd_psndoc.code as jobid,tectittle.name as tectittlename,tittlerank.name as tectittlerank from bd_psndoc left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc  left join bd_defdoc tectittle on hi_psndoc_title.pk_techposttitle = tectittle.pk_defdoc left join bd_defdoc tittlerank on hi_psndoc_title.titlerank =tittlerank.pk_defdoc where bd_psndoc.enablestate =2') as ncinfo left hash join RewardPointDB.dbo.RewardPointsStandard as std on std.CheckItem=ncinfo.tectittlerank"
        query_item = ["(std.RewardPointsTypeID=6 or std.RewardPointsTypeID is null)",
                      f"ncinfo.jobid in ({all_id_tupe})"]
        techtittle_df = pd.read_sql(sql=base_sql + " where " + ' and '.join(query_item), con=self.db_mssql)
        # A 管理分表
        mssql_base_sql = '''select dt.*,dt.总可用管理积分-isnull(od.pointuse,0)  as 现有管理积分 from (select dt.JobId,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.IsAccounted=0 then dt.BonusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.IsAccounted=0 then dt.MinusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.IsAccounted=1 then dt.ChangeAmount else 0 end) as 现有A分,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.BonusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.MinusPoints else 0 end) as 总可用管理积分,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 and dt.AssessmentDate>'2019-12-31 00:00:00' then dt.BonusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 and dt.AssessmentDate>'2019-12-31 00:00:00' then dt.MinusPoints else 0 end) as 年度管理积分,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 then dt.BonusPoints else 0 end) as 总获得A分,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.BonusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.MinusPoints else 0 end) as 总获得管理积分 from RewardPointDB.dbo.RewardPointsDetail dt where dt.DataStatus=0 group by dt.JobId) as dt left join (select JobId,sum(case when od.DataStatus=0 and od.OrderStatus in (0,1,2) then od.TotalPrice else 0 end) as pointuse from RewardPointDB.dbo.PointOrder od group by JobId) od on od.JobId=dt.JobId where {0[1]}'''
        sql_item = [datetime_string(datetime.datetime(year=today.year - 1, month=12, day=31)), ]
        query_item = [f"dt.JobId in ({all_id_tupe})"]
        sql_item.append(' and '.join(query_item))
        mssql_sql = mssql_base_sql.format(sql_item)
        pointdetail = pd.read_sql(mssql_sql, self.db_mssql)
        maninfo_df = pd.merge(maninfo_df, pointdetail, left_on='工号', right_on='JobId', how='left').fillna(0)
        #  工龄表
        ServingAge_base_sql = '''select bd_psndoc.code,min(hi_psndoc_psnchg.begindate) as begindate 
        from hi_psndoc_psnchg join bd_psndoc on hi_psndoc_psnchg.pk_psndoc=bd_psndoc.pk_psndoc 
        where (hi_psndoc_psnchg.enddate>'2004-01-01' or hi_psndoc_psnchg.enddate is null)and bd_psndoc.code in ({}) group by bd_psndoc.code '''
        ServingAge_sql = ServingAge_base_sql.format(all_id_tupe)
        manServing_df = pd.read_sql(ServingAge_sql, self.db_nc)
        #  职务表
        jobrank_base_sql = '''select ncinfo.*,std.PointsAmount from openquery(NC,'select hi_psnjob.begindate,hi_psnjob.enddate,bd_psndoc.code, om_jobrank.jobrankname as 职等 from hi_psnjob join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc join om_jobrank on om_jobrank.pk_jobrank = hi_psnjob.pk_jobrank where hi_psnjob.ismainjob =''Y'' and bd_psndoc.enablestate =2 and (hi_psnjob.enddate>''2018-01-01'' or hi_psnjob.endflag=''N'') and bd_psndoc.code in ({}) order by bd_psndoc.code,hi_psnjob.begindate') as ncinfo inner hash join RewardPointDB.dbo.RewardPointsStandard  as std on ncinfo.职等=std.CheckItem where std.DataStatus=0'''
        tempidlist = []
        for _ii in all_id:
            tempidlist.append("\'\'" + _ii + "\'\'")
        all_id_tupe = ','.join(tempidlist)
        jobrank_sql = jobrank_base_sql.format(all_id_tupe)
        jobrank_df = pd.read_sql(jobrank_sql, self.db_mssql)

        # 填充
        for man in all_id:
            man_select = maninfo_df['工号'] == man
            # 学历积分
            SchoolPoints, education, schoolname = 0, '', ''
            if len(school_df.loc[school_df['jobid'] == man, :]) > 0:
                education = school_df.loc[school_df['jobid'] == man, "educationname"].values[0]
                schoolname = school_df.loc[school_df['jobid'] == man, "schoolname"].values[0]
                SchoolPoints = school_df.loc[school_df['jobid'] == man, "PointsAmount"].values[0]
                if pd.isna(SchoolPoints): SchoolPoints = 0
                if education == '本科' and not pd.isna(schoolname):
                    if schoolname in self.HighSchoolList: SchoolPoints += 500
            maninfo_df.loc[man_select, '学历积分'] = SchoolPoints
            maninfo_df.loc[man_select, '学历'] = education
            maninfo_df.loc[man_select, '学校'] = schoolname
            # 职称积分
            TittlePoint = 0
            tittle_rank = ''
            if len(techtittle_df.loc[techtittle_df['jobid'] == man, :]) > 0:
                for _tn, _tr, _p in zip(techtittle_df.loc[techtittle_df['jobid'] == man, 'tectittlename'],
                                        techtittle_df.loc[techtittle_df['jobid'] == man, 'tectittlerank'],
                                        techtittle_df.loc[techtittle_df['jobid'] == man, 'PointsAmount']):
                    if pd.isna(_p): _p = 0
                    temp_tittlepoint = _p
                    if temp_tittlepoint > TittlePoint:
                        TittlePoint = temp_tittlepoint
                        tittle_rank = _tr
            maninfo_df.loc[man_select, '职称等级'] = tittle_rank
            # 工龄积分
            serving_begindate = datetime.datetime.strptime(
                manServing_df.loc[manServing_df['CODE'] == man, 'BEGINDATE'].values[0], "%Y-%m-%d")  # 取起始时间

            if serving_begindate.__le__(self.serving_count_time):  # 如果早于2004-01-01,那么从2004-01-01开始算
                serving_begindate = self.serving_count_time
            years, months = countTime_NewYear(beginDate=serving_begindate,
                                              endDate=datetime.datetime(year=today.year, month=12, day=30))
            ServingAgePoints = int((years + months / 12) * 2000)
            # 职务积分
            jobrankpoint = 0  #
            man_workinfo = jobrank_df.loc[jobrank_df['CODE'] == man, :].reset_index()  # 取个人的工作信息
            if len(man_workinfo) > 0:
                jobrank_begindate = datetime.datetime.strptime(man_workinfo.loc[0, 'BEGINDATE'], "%Y-%m-%d")  # 取起始时间
                if jobrank_begindate.__le__(self.jobrank_count_time):  # 如果早于jobrank_count_time,那么从2018-01-01开始算
                    jobrank_begindate = self.jobrank_begin_time
                man_workinfo.loc[0, 'BEGINDATE'] = jobrank_begindate  # 填回去
                for work_index in man_workinfo.index:
                    if pd.isna(man_workinfo.loc[work_index, 'ENDDATE']):
                        # temp_enddate = today
                        temp_enddate = datetime.datetime(year=today.year, month=12, day=31)
                    else:
                        temp_enddate = datetime.datetime.strptime(man_workinfo.loc[work_index, 'ENDDATE'], "%Y-%m-%d")
                    if isinstance(man_workinfo.loc[work_index, 'BEGINDATE'], str):
                        temp_begindate = datetime.datetime.strptime(man_workinfo.loc[work_index, 'BEGINDATE'],
                                                                    "%Y-%m-%d")
                    else:
                        temp_begindate = man_workinfo.loc[work_index, 'BEGINDATE']

                    months = sub_datetime_Bydayone(beginDate=temp_begindate, endDate=temp_enddate)
                    temp_standard = man_workinfo.loc[work_index, 'PointsAmount']
                    jobrankpoint += np.around(temp_standard * months / 12)
            maninfo_df.loc[man_select, '固定积分'] = SchoolPoints + TittlePoint + ServingAgePoints + jobrankpoint
            maninfo_df.loc[man_select, '总累计积分'] = maninfo_df.loc[man_select, '固定积分'] + maninfo_df.loc[
                man_select, '总获得管理积分']
            maninfo_df.loc[man_select, '年度累计积分'] = maninfo_df.loc[man_select, '固定积分'] + maninfo_df.loc[
                man_select, '年度管理积分']
        return totalLength, maninfo_df

    def _base_query_FixedPoints(self, data_in: dict) -> (int, pd.DataFrame):
        today = datetime.datetime.today()
        query_item = ["hi_psnjob.endflag ='N'", "hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                      "bd_psndoc.enablestate =2", "hi_psnjob. poststat='Y'", "bd_psncl.name in ('全职','退休返聘','试用期员工')"]
        if data_in.get('jobid'):
            query_item.append(f"bd_psndoc.code = '{data_in.get('jobid')}'")
        if data_in.get('name'):
            query_item.append(f"bd_psndoc.name like '%{data_in.get('name')}%'")
        if data_in.get('pageSize') and data_in.get('page'):
            maninfo_base_sql = '''
            select * 
            from (
            select rownum as rowno,
            bd_psndoc.name as 姓名,bd_psndoc.code as 工号,org_dept.name as 部门,om_job.jobname  as 职务,org_adminorg.name as 组织
            from hi_psnjob
            left join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
            left join org_dept on org_dept.pk_dept =hi_psnjob.pk_dept
            left join om_job on om_job.pk_job  = hi_psnjob.pk_job 
            left join org_adminorg on org_adminorg.pk_adminorg  =hi_psnjob.pk_org
            left join bd_psncl on bd_psncl.pk_psncl=hi_psnjob. pk_psncl  {0[0]}) temptable
            where temptable.rowno >{0[1]}*({0[2]}-1) and temptable.rowno <={0[1]}*{0[2]} '''
            sql_item = [" where " + ' and '.join(query_item), data_in.get('pageSize'), data_in.get('page')]
        else:
            maninfo_base_sql = '''
            select 
            bd_psndoc.name as 姓名,bd_psndoc.code as 工号,org_dept.name as 部门,om_job.jobname  as 职务,org_adminorg.name as 组织
            from hi_psnjob
            left join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc
            left join org_dept on org_dept.pk_dept =hi_psnjob.pk_dept
            left join om_job on om_job.pk_job  = hi_psnjob.pk_job 
            left join org_adminorg on org_adminorg.pk_adminorg  =hi_psnjob.pk_org
            left join bd_psncl on bd_psncl.pk_psncl=hi_psnjob. pk_psncl  {0[0]} '''
            sql_item = [" where " + ' and '.join(query_item)]
        maninfo_sql = maninfo_base_sql.format(sql_item)
        maninfo_df = pd.read_sql(sql=maninfo_sql, con=self.db_nc)
        length_base_sql = "select count(rownum) as res from hi_psnjob left join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc left join bd_psncl on bd_psncl.pk_psncl=hi_psnjob. pk_psncl {0[0]}"
        # 计算数据长度
        totalLength = pd.read_sql(sql=length_base_sql.format(sql_item), con=self.db_nc).loc[0, 'RES']
        tempidlist = []
        all_id = maninfo_df['工号'].tolist()
        res_df = pd.DataFrame(
            columns=('姓名', '工号', '部门', '组织', '职务', '职称积分', '学历积分', '工龄积分', '职务积分', '学历', '入职时间', '职称'))
        res_df['工号'] = all_id
        if len(all_id) == 0:
            return 0, res_df
        for _ii in all_id:
            tempidlist.append("\'" + _ii + "\'")
        all_id_tupe = ','.join(tempidlist)
        #  学历
        base_sql = "select ncinfo.jobid,ncinfo.educationname,ncinfo.schoolname,std.PointsAmount from openquery(NC,'select bd_psndoc.code as jobid,c1.name as educationname,edu.school as schoolname from bd_psndoc left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc left join bd_defdoc c1 on edu.education = c1.pk_defdoc where bd_psndoc.enablestate =2 and edu.lasteducation=''Y''') as ncinfo left hash join RewardPointDB.dbo.RewardPointsStandard as std on std.CheckItem=ncinfo.educationname"
        query_item = ["(std.RewardPointsTypeID=7 or std.RewardPointsTypeID is null)",
                      f"ncinfo.jobid in ({all_id_tupe})"]
        school_df = pd.read_sql(sql=base_sql + " where " + ' and '.join(query_item), con=self.db_mssql)
        #  职称
        base_sql = "select ncinfo.jobid,ncinfo.tectittlename,ncinfo.tectittlerank,std.PointsAmount from openquery(NC,'select bd_psndoc.code as jobid,tectittle.name as tectittlename,tittlerank.name as tectittlerank from bd_psndoc left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc  left join bd_defdoc tectittle on hi_psndoc_title.pk_techposttitle = tectittle.pk_defdoc left join bd_defdoc tittlerank on hi_psndoc_title.titlerank =tittlerank.pk_defdoc where bd_psndoc.enablestate =2') as ncinfo left hash join RewardPointDB.dbo.RewardPointsStandard as std on std.CheckItem=ncinfo.tectittlerank"
        query_item = ["(std.RewardPointsTypeID=6 or std.RewardPointsTypeID is null)",
                      f"ncinfo.jobid in ({all_id_tupe})"]
        techtittle_df = pd.read_sql(sql=base_sql + " where " + ' and '.join(query_item), con=self.db_mssql)
        #  工龄表
        ServingAge_base_sql = '''select bd_psndoc.code, min(hi_psndoc_psnchg.begindate) as begindate 
        from hi_psndoc_psnchg join bd_psndoc on hi_psndoc_psnchg.pk_psndoc=bd_psndoc.pk_psndoc 
        where (hi_psndoc_psnchg.enddate>'2004-01-01' or hi_psndoc_psnchg.enddate is null) and bd_psndoc.code in ({}) group by bd_psndoc.code '''
        ServingAge_sql = ServingAge_base_sql.format(all_id_tupe)
        manServing_df = pd.read_sql(ServingAge_sql, self.db_nc)
        #  职务表
        jobrank_base_sql = '''select ncinfo.*,std.PointsAmount from openquery(NC,'select hi_psnjob.begindate,hi_psnjob.enddate,bd_psndoc.code,om_jobrank.jobrankname as 职等 from hi_psnjob join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc join om_jobrank on om_jobrank.pk_jobrank = hi_psnjob.pk_jobrank  where hi_psnjob.ismainjob =''Y'' and bd_psndoc.enablestate =2 and (hi_psnjob.enddate>''2018-01-01'' or hi_psnjob.endflag=''N'') and bd_psndoc.code in ({}) order by bd_psndoc.code,hi_psnjob.begindate') as ncinfo inner hash join RewardPointDB.dbo.RewardPointsStandard  as std on ncinfo.职等=std.CheckItem where std.DataStatus=0 '''
        tempidlist = []
        for _ii in all_id:
            tempidlist.append("\'\'" + _ii + "\'\'")
        all_id_tupe = ','.join(tempidlist)
        jobrank_sql = jobrank_base_sql.format(all_id_tupe)
        jobrank_df = pd.read_sql(jobrank_sql, self.db_mssql)
        for _index in res_df.index:
            man = res_df.loc[_index, '工号']
            man_select = maninfo_df['工号'] == man
            for _item in ['姓名', '部门', '职务', '组织']:
                if len(maninfo_df.loc[man_select, _item]) == 1:
                    res_df.loc[_index, _item] = maninfo_df.loc[man_select, _item].values[0]
            #  学历积分
            SchoolPoints = 0
            education = ''
            if len(school_df.loc[school_df['jobid'] == man, :]) > 0:
                education = school_df.loc[school_df['jobid'] == man, "educationname"].values[0]
                schoolname = school_df.loc[school_df['jobid'] == man, "schoolname"].values[0]
                SchoolPoints = school_df.loc[school_df['jobid'] == man, "PointsAmount"].values[0]
                if pd.isna(SchoolPoints): SchoolPoints = 0
                if education == '本科' and not pd.isna(schoolname):
                    if schoolname in self.HighSchoolList: SchoolPoints += 500
            res_df.loc[_index, '学历积分'] = SchoolPoints
            res_df.loc[_index, '学历'] = education
            # 职称积分
            TittlePoint = 0
            tech_tittle = ''
            if len(techtittle_df.loc[techtittle_df['jobid'] == man, :]) > 0:
                for _tn, _tr, _p in zip(techtittle_df.loc[techtittle_df['jobid'] == man, 'tectittlename'],
                                        techtittle_df.loc[techtittle_df['jobid'] == man, 'tectittlerank'],
                                        techtittle_df.loc[techtittle_df['jobid'] == man, 'PointsAmount']):
                    if pd.isna(_p): _p=0
                    temp_tittlepoint = _p
                    if temp_tittlepoint > TittlePoint:
                        TittlePoint = temp_tittlepoint
                        tech_tittle = _tn
            res_df.loc[_index, '职称积分'] = TittlePoint
            res_df.loc[_index, '职称'] = tech_tittle
            # 工龄积分
            serving_begindate = datetime.datetime.strptime(
                manServing_df.loc[manServing_df['CODE'] == man, 'BEGINDATE'].values[0], "%Y-%m-%d")  # 取起始时间
            res_df.loc[_index, '入职时间'] = manServing_df.loc[manServing_df['CODE'] == man, 'BEGINDATE'].values[0]
            serving_count_time = datetime.datetime(year=2014, month=1, day=1)
            if serving_begindate.__le__(serving_count_time):  # 如果早于2004-01-01,那么从2004-01-01开始算
                serving_begindate = serving_count_time
            years, months = countTime_NewYear(beginDate=serving_begindate,
                                              endDate=datetime.datetime(year=today.year, month=12, day=30))
            ServingAgePoints = int((years + months / 12) * 2000)
            res_df.loc[_index, '工龄积分'] = ServingAgePoints
            # 职务积分
            jobrankpoint = 0  #
            man_workinfo = jobrank_df.loc[jobrank_df['CODE'] == man, :].reset_index()  # 取个人的工作信息
            if len(man_workinfo) > 0:
                jobrank_begindate = datetime.datetime.strptime(man_workinfo.loc[0, 'BEGINDATE'], "%Y-%m-%d")  # 取起始时间
                if jobrank_begindate.__le__(self.jobrank_count_time):  # 如果早于2018-08-01,那么从2018-01-01开始算
                    jobrank_begindate = self.jobrank_begin_time
                man_workinfo.loc[0, 'BEGINDATE'] = jobrank_begindate  # 填回去
                for work_index in man_workinfo.index:
                    if pd.isna(man_workinfo.loc[work_index, 'ENDDATE']):
                        temp_enddate = datetime.datetime(year=today.year, month=12, day=31)
                    else:
                        temp_enddate = datetime.datetime.strptime(man_workinfo.loc[work_index, 'ENDDATE'], "%Y-%m-%d")
                    if isinstance(man_workinfo.loc[work_index, 'BEGINDATE'], str):
                        temp_begindate = datetime.datetime.strptime(man_workinfo.loc[work_index, 'BEGINDATE'],
                                                                    "%Y-%m-%d")
                    else:
                        temp_begindate = man_workinfo.loc[work_index, 'BEGINDATE']
                    months = sub_datetime_Bydayone(beginDate=temp_begindate, endDate=temp_enddate)
                    temp_standard = man_workinfo.loc[work_index, 'PointsAmount']
                    jobrankpoint += np.around(temp_standard * months / 12)
            res_df.loc[_index, '职务积分'] = jobrankpoint
        return totalLength, res_df

    def _base_query_goods(self, data_in: dict) -> (int, pd.DataFrame):
        if data_in:  # 不为空则按照条件查询
            query_item = ["goods.DataStatus=0"]  # 查询条件
            # 商品名称
            if data_in.get("Name"):
                goodName = "\'%" + data_in.get('Name') + "%\'"
                query_item.append(f"goods.Name like {goodName}")
            # 商品编码
            if data_in.get("GoodsCode"):
                query_item.append(f"goods.GoodsCode in ({data_in.get('GoodsCode')})")
            # 商品状态
            if data_in.get("Status") or data_in.get("Status") == 0:
                query_item.append(f"goods.Status = {data_in.get('Status')}")
            query_sql = " where " + ' and '.join(query_item)
            # 分页
            base_sql = "select goods.GoodsCode,goods.Name,goods.PictureUrl,goods.PointCost,goods.Status,goods.GoodsID,goods.MeasureUnit,stkin.TotalIn,stkout.TotalLock,stkout.TotalOut from Goods goods left join (select GoodsID,sum(case when stkin.DataStatus=0 and stkin.ChangeType=0 then stkin.ChangeAmount else 0 end) as TotalIn from StockInDetail stkin group by GoodsID) stkin on stkin.GoodsID = goods.GoodsID left join (select GoodsID,sum(case when stkout.DataStatus=0 and stkout.ChangeType=0 then stkout.ChangeAmount else 0 end) as TotalOut,sum(case when stkout.DataStatus=0 and stkout.ChangeType=1 then stkout.ChangeAmount else 0 end) as TotalLock from StockOutDetail stkout group by GoodsID)stkout on stkout.GoodsID = goods.GoodsID  {0[0]}  order by goods.GoodsID asc {0[1]}"
            sql_item = [query_sql]
            if not (data_in.get("page") and data_in.get("pageSize")):  # 不分页
                sql_item.append('')
            else:
                page_sql = " offset {0[0]}*({0[1]}-1) rows fetch next {0[0]} rows only".format(
                    [data_in.get("pageSize"), data_in.get("page")])
                sql_item.append(page_sql)
            # 拼起来
            sql = base_sql.format(sql_item)
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
            # 计算总行数
            totalLength_sql = "select count(GoodsID) as res from [RewardPointDB].[dbo].[Goods] "
            lensql = totalLength_sql + " where " + ' and '.join(query_item)
            totalLength = pd.read_sql(sql=lensql, con=self.db_mssql).loc[0, 'res']
        else:
            sql = "select * from [RewardPointDB].[dbo].[Goods] where DataStatus=0"
            res_df = pd.read_sql(sql=sql, con=self.db_mssql)
            # 计算总行数
            totalLength_sql = "select count(GoodsID) as res from [RewardPointDB].[dbo].[Goods] "
            totalLength = pd.read_sql(sql=totalLength_sql, con=self.db_mssql).loc[0, 'res']

        return totalLength, res_df

    def _base_query_order(self, data_in: dict) -> (int, pd.DataFrame):
        query_item = ["PointOrder.DataStatus=0"]
        if data_in.get("Operator"):
            query_item.append(f"PointOrder.JobId={data_in.get('Operator')}")
        if data_in.get("OrderStatus") or data_in.get("OrderStatus") == 0:
            query_item.append(f"PointOrder.OrderStatus={data_in.get('OrderStatus')}")
        query_sql = " where " + ' and '.join(query_item)
        base_sql = '''
        select PointOrder.PointOrderID,PointOrder.CreationDate,PointOrder.JobId,PointOrder.OrderStatus,PointOrder.TotalNum,
        PointOrder.TotalPrice,NCDB.NAME,NCDB.ORGNAME,NCDB.DEPTNAME 
        from RewardPointDB.dbo.PointOrder 
        inner hash join 
        openquery(NC,'select bd_psndoc.code,bd_psndoc.name,org_dept.name AS DEPTNAME,org_adminorg.name AS ORGNAME from hi_psnjob
                    left join bd_psndoc on bd_psndoc.pk_psndoc=hi_psnjob.pk_psndoc
                    left join org_dept on org_dept.pk_dept= hi_psnjob.pk_dept
                    left join org_adminorg on org_adminorg.pk_adminorg  =hi_psnjob.pk_org
                    where hi_psnjob.endflag =''N'' and hi_psnjob.ismainjob =''Y'' and hi_psnjob.lastflag  =''Y'' ')
                    as NCDB on NCDB.CODE = PointOrder.JobId {0[0]} 
        order by PointOrder.PointOrderID desc {0[1]}'''
        if not (data_in.get("page") and data_in.get("pageSize")):  # 不分页
            sql_item = [query_sql, '']
        else:
            page_sql = " offset {0[0]}*({0[1]}-1) rows fetch next {0[0]} rows only".format(
                [data_in.get("pageSize"), data_in.get("page")])
            sql_item = [query_sql, page_sql]
        # 拼起来
        sql = base_sql.format(sql_item)
        res_df = pd.read_sql(sql=sql, con=self.db_mssql)
        totalLength_sql = "select count(PointOrderID) as res from [RewardPointDB].[dbo].[PointOrder] {0[0]}".format(
            sql_item)
        totalLength = pd.read_sql(sql=totalLength_sql, con=self.db_mssql).loc[0, 'res']
        return totalLength, res_df

    def _base_query_orderDetail(self, data_in: dict) -> pd.DataFrame:
        query_item = ['ordergoods.DataStatus=0']
        orderID = data_in.get('PointOrderID')
        query_item.append(f"orders.PointOrderID in ({orderID})")
        _base_sql = '''
        select orders.PointOrderID,goods.PictureUrl,goods.GoodsCode,goods.Status,goods.Name,goods.PointCost,
        ordergoods.OrderGoodsAmount
        from RewardPointDB.dbo.PointOrderGoods as ordergoods 
        join RewardPointDB.dbo.PointOrder as orders on orders.PointOrderID=ordergoods.PointOrderID
        join RewardPointDB.dbo.Goods as goods on goods.GoodsID=ordergoods.GoodsID {}'''
        sql = _base_sql.format(" where " + " and ".join(query_item))
        orderDetail_df = pd.read_sql(sql, con=self.db_mssql)
        return orderDetail_df

    def _base_query_FixedPoints_ByYear(self, data_in: dict) -> int:
        sql = f"select sum(dt.BonusPoints)-sum(dt.MinusPoints) as AnnualSum from RewardPointDB.dbo.RewardPointsDetail dt where dt.DataStatus=0 and dt.RewardPointsTypeID =3 and dt.AssessmentDate>'{data_in.get('year')}-01-01 00:00:00' and dt.AssessmentDate<'{data_in.get('year') + 1}-01-01 00:00:00' and dt.JobId='{data_in.get('jobid')}'"
        res_df = pd.read_sql(sql=sql, con=self.db_mssql)
        return res_df.loc[0, 'AnnualSum']

    def _base_query_FixedPointDetail(self, data_in: dict) -> dict:
        today = datetime.datetime.today()  # 今天
        man_data = {}
        man = data_in.get('jobid')
        man_data['工号'] = man
        maninfo_base_sql = "select bd_psndoc.name as 姓名,bd_psndoc.code as 工号 from hi_psnjob join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc"
        query_item = ["hi_psnjob.endflag ='N'", "hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                      "bd_psndoc.enablestate =2", f"bd_psndoc.code='{man}'"]
        maninfo_sql = maninfo_base_sql + " where " + ' and '.join(query_item)
        maninfo_df = pd.read_sql(sql=maninfo_sql, con=self.db_nc)
        if len(maninfo_df) == 0:
            return man_data
        #  学历
        base_sql = "select ncinfo.jobid,ncinfo.educationname,ncinfo.schoolname,std.PointsAmount from openquery(NC,'select bd_psndoc.code as jobid,c1.name as educationname,edu.school as schoolname from bd_psndoc left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc left join bd_defdoc c1 on edu.education = c1.pk_defdoc where bd_psndoc.enablestate =2 and edu.lasteducation=''Y''') as ncinfo left hash join RewardPointDB.dbo.RewardPointsStandard as std on std.CheckItem=ncinfo.educationname"
        query_item = ["(std.RewardPointsTypeID=7 or std.RewardPointsTypeID is null)", f"ncinfo.jobid = '{man}'"]
        school_df = pd.read_sql(sql=base_sql + " where " + ' and '.join(query_item), con=self.db_mssql)
        #  职称
        base_sql = "select ncinfo.jobid,ncinfo.tectittlename,ncinfo.tectittlerank,std.PointsAmount from openquery(NC,'select bd_psndoc.code as jobid,tectittle.name as tectittlename,tittlerank.name as tectittlerank from bd_psndoc left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc  left join bd_defdoc tectittle on hi_psndoc_title.pk_techposttitle = tectittle.pk_defdoc left join bd_defdoc tittlerank on hi_psndoc_title.titlerank =tittlerank.pk_defdoc where bd_psndoc.enablestate =2') as ncinfo left hash join RewardPointDB.dbo.RewardPointsStandard as std on std.CheckItem=ncinfo.tectittlerank"
        query_item = ["(std.RewardPointsTypeID=6 or std.RewardPointsTypeID is null)", f"ncinfo.jobid = '{man}'"]
        techtittle_df = pd.read_sql(sql=base_sql + " where " + ' and '.join(query_item), con=self.db_mssql)
        #  学历积分
        SchoolPoints = 0
        is985211 = False
        education = ''
        School_data, serving_data = {}, {}
        if len(school_df.loc[school_df['jobid'] == man, :]) > 0:
            education = school_df.loc[school_df['jobid'] == man, "educationname"].values[0]
            schoolname = school_df.loc[school_df['jobid'] == man, "schoolname"].values[0]
            SchoolPoints = school_df.loc[school_df['jobid'] == man, "PointsAmount"].values[0]
            if pd.isna(SchoolPoints): SchoolPoints = 0
            if education == '本科' and not pd.isna(schoolname):
                if schoolname in self.HighSchoolList:
                    SchoolPoints += 500
                    is985211 = True
        School_data['schoolPoints'] = SchoolPoints
        School_data['education'] = education
        School_data['is985211'] = is985211
        # 职称积分
        Tittle_data = {'tittleRankPoint': 0}
        if len(techtittle_df.loc[techtittle_df['jobid'] == man, :]) > 0:
            for _tn, _tr, _p in zip(techtittle_df.loc[techtittle_df['jobid'] == man, 'tectittlename'],
                                    techtittle_df.loc[techtittle_df['jobid'] == man, 'tectittlerank'],
                                    techtittle_df.loc[techtittle_df['jobid'] == man, 'PointsAmount']):
                if pd.isna(_p): _p=0
                temp_tittlepoint = _p
                if temp_tittlepoint > Tittle_data['tittleRankPoint']:
                    Tittle_data['tittleRankPoint'] = temp_tittlepoint
                    Tittle_data['tectittle'] = _tn
                    Tittle_data['tittleRank'] = _tr
        ServingAge_base_sql = '''
        select 
        hi_psndoc_psnchg.begindate,hi_psndoc_psnchg.enddate
        from hi_psndoc_psnchg
        join bd_psndoc on hi_psndoc_psnchg.pk_psndoc=bd_psndoc.pk_psndoc
        where
        (hi_psndoc_psnchg.enddate>'2004-01-01' or hi_psndoc_psnchg.enddate is null)
        and bd_psndoc.enablestate =2
        and bd_psndoc.code='{}'
        order by bd_psndoc.code,hi_psndoc_psnchg.begindate
                    '''
        ServingAge_sql = ServingAge_base_sql.format(man)
        manServing_df = pd.read_sql(ServingAge_sql, self.db_nc)
        #  工龄积分
        serving_begindate = datetime.datetime.strptime(manServing_df.loc[0, 'BEGINDATE'], "%Y-%m-%d")  # 取起始时间

        if serving_begindate.__le__(self.serving_count_time):  # 如果早于2004-01-01,那么从2004-01-01开始算
            serving_begindate = self.serving_count_time
        years, months = countTime_NewYear(beginDate=serving_begindate,
                                          endDate=datetime.datetime(year=today.year, month=12, day=30))
        ServingAgePoints = int((years + months / 12) * 2000)
        serving_data['begindate'] = manServing_df.loc[0, 'BEGINDATE']
        serving_data['servingAge_years'] = years
        serving_data['servingAge_months'] = months
        serving_data['servingAgePoints'] = ServingAgePoints
        #  职务积分
        jobrank_data = []  # 职务积分详情容器
        jobrank_base_sql = '''select ncinfo.*,std.PointsAmount from openquery(NC,'select hi_psnjob.begindate,hi_psnjob.enddate,bd_psndoc.code,om_jobrank.jobrankname as 职等 from hi_psnjob join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc join om_jobrank on om_jobrank.pk_jobrank = hi_psnjob.pk_jobrank  where hi_psnjob.ismainjob =''Y'' and bd_psndoc.enablestate =2 and (hi_psnjob.enddate>''2018-01-01'' or hi_psnjob.endflag=''N'') and bd_psndoc.code in ({}) order by bd_psndoc.code,hi_psnjob.begindate') as ncinfo inner hash join RewardPointDB.dbo.RewardPointsStandard  as std on ncinfo.职等=std.CheckItem where std.DataStatus=0 '''
        tempidlist = []
        all_id = [str(man)]
        for _ii in all_id:
            tempidlist.append("\'\'" + _ii + "\'\'")
        all_id_tupe = ','.join(tempidlist)
        jobrank_sql = jobrank_base_sql.format(all_id_tupe)
        jobrank_df = pd.read_sql(jobrank_sql, self.db_mssql)

        jobrank_begindate = datetime.datetime.strptime(jobrank_df.loc[0, 'BEGINDATE'], "%Y-%m-%d")  # 取起始时间
        if jobrank_begindate.__le__(self.jobrank_count_time):  # 如果早于2018-01-01,那么从2018-01-01开始算
            jobrank_begindate = self.jobrank_begin_time
        jobrank_df.loc[0, 'BEGINDATE'] = jobrank_begindate  # 填回去
        for work_index in jobrank_df.index:
            if pd.isna(jobrank_df.loc[work_index, 'ENDDATE']):
                temp_enddate = datetime.datetime(year=today.year, month=12, day=31)
                islatest = True
            else:
                temp_enddate = datetime.datetime.strptime(jobrank_df.loc[work_index, 'ENDDATE'], "%Y-%m-%d")
                islatest = False
            if isinstance(jobrank_df.loc[work_index, 'BEGINDATE'], str):
                temp_begindate = datetime.datetime.strptime(jobrank_df.loc[work_index, 'BEGINDATE'], "%Y-%m-%d")
            else:
                temp_begindate = jobrank_df.loc[work_index, 'BEGINDATE']
            months = sub_datetime_Bydayone(beginDate=temp_begindate, endDate=temp_enddate)
            temp_jobrankpoint = 0
            jobrank = jobrank_df.loc[work_index, '职等']
            if len(jobrank) != 0:  # 如果职等字段不为空
                temp_standard = jobrank_df.loc[work_index, 'PointsAmount']
                temp_jobrankpoint = np.around(temp_standard * months / 12)
            jobrank_data.append({'begindate': datetime_string(temp_begindate, timeType="%Y-%m-%d"),
                                 'enddate': datetime_string(temp_enddate, timeType="%Y-%m-%d"),
                                 'islatest': islatest,
                                 'jobrank': jobrank,
                                 'jobname': jobrank_df.loc[work_index, '职等'],
                                 'jobrankpoint': temp_jobrankpoint})

        # 存起来
        man_data['学历积分'] = School_data
        man_data['职称积分'] = Tittle_data
        man_data['工龄积分'] = serving_data
        man_data['职务积分'] = jobrank_data

        return man_data

    def query_RewardPointSummary(self, data_in: dict) -> (int, pd.DataFrame):
        return self._base_query_RewardPointSummary(data_in)

    def export_RewardPointSummary(self, data_in: dict) -> str:
        _, res_df = self._base_query_RewardPointSummary(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=data_in.get("Operator"))

    def query_FixedPointDetail(self, data_in: dict) -> dict:
        return self._base_query_FixedPointDetail(data_in=data_in)

    def query_rewardPoint(self, data_in: dict) -> (int, pd.DataFrame):
        '''
        返回积分详情查询信息
        :param data_in:
        :return:
        '''
        return self._base_query_rewardPointDetail(data_in=data_in)

    def delete_rewardPoint(self, data_in: dict) -> bool:
        base_sql = "update [RewardPointDB].[dbo].[RewardPointsDetail] set DataStatus=1 where RewardPointsdetailID = {}"
        sql = base_sql.format(data_in.get("RewardPointsdetailID"))
        cur = self.db_mssql.cursor()
        cur.execute(sql)
        self.db_mssql.commit()
        return True

    def export_rewardPoint(self, data_in: dict) -> str:
        '''
        包装积分详情信息，生成EXCEL并返回下载链接
        :param data_in:
        :return:
        '''
        _, res_df = self._base_query_rewardPointDetail(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=data_in.get("Operator"))

    def import_rewardPoint_onesql(self, data_in: dict, file_df: pd.DataFrame) -> bool:
        rewardPointType_df = pd.read_sql(
            "select RewardPointsTypeID,Name from RewardPointDB.dbo.RewardPointsType where DataStatus=0",
            con=self.db_mssql)
        base_sql = '''
        insert into [RewardPointDB].[dbo].[RewardPointsDetail] (
         ChangeType,CreatedBy,JobId,DepartmentLv1,DepartmentLv2,DepartmentLv3,Post,Name,RewardPointsTypeID,BonusPoints,
         MinusPoints,Reason,Proof,ReasonType,FunctionalDepartment,Submit,AssessmentDate ) values{}
        '''
        sql_values = []
        value_check = ['工号', '一级部门', '二级部门', '三级部门', '职务名称',
                       '姓名', 'A分/B分', '加分',
                       '减分', '加减分理由', '加减分依据', '理由分类', '职能部门/所在部门管理', '提交部门',
                       '考核日期']
        for i in file_df.index:
            values_item = ['0', data_in.get("Operator")]
            for vi in value_check:
                value = file_df.loc[i, vi]
                if pd.isna(value):
                    values_item.append("\'" + '' + "\'")
                elif vi == 'A分/B分':  # 连RewardPointsType表查积分类型ID
                    RewardPointsTypeID = rewardPointType_df.loc[
                        rewardPointType_df['Name'] == value, 'RewardPointsTypeID'].values[0]
                    values_item.append(str(RewardPointsTypeID))
                elif isinstance(value, pd.Timestamp):  # 如果是Timestamp转字符串
                    values_item.append("\'" + datetime_string(pd.to_datetime(value, "%Y-%m-%d %H:%M:%S")) + "\'")
                elif isinstance(value, str):  # 如果是字符串 加上引号
                    values_item.append("\'" + value + "\'")
                else:
                    values_item.append(str(value))
            sql_values.append("(" + ','.join(values_item) + ")")
        sql = base_sql.format(','.join(sql_values))
        cur = self.db_mssql.cursor()
        try:
            cur.execute(sql)
            self.db_mssql.commit()
            return True
        except Exception as e:
            return False

    @verison_warning
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
        for _index in file_df.index.tolist():
            sql_item = ['ChangeType', 'CreatedBy']
            sql_values = [0, data_in.get("Operator")]
            for (k, v) in column_dic.items():
                if k in columns:
                    sql_item.append(v)
                    value = file_df.loc[_index, k]
                    if pd.isna(value):
                        sql_values.append("\'" + '' + "\'")
                        continue
                    if k == 'A分/B分':  # 连RewardPointsType表查积分类型ID
                        RewardPointsTypeID = rewardPointType_df.loc[
                            rewardPointType_df['Name'] == value, 'RewardPointsTypeID'].values[0]
                        sql_values.append(RewardPointsTypeID)
                        continue
                    if isinstance(value, pd.Timestamp):  # 如果是Timestamp转字符串
                        sql_values.append("\'" + datetime_string(pd.to_datetime(value, "%Y-%m-%d %H:%M:%S")) + "\'")
                        continue
                    if isinstance(value, str):  # 如果是字符串 加上引号
                        sql_values.append("\'" + value + "\'")
                    else:
                        sql_values.append(value)
                else:
                    pass
            sql = base_sql.format(','.join(sql_item), ','.join(list(map(str, sql_values))))
            cur.execute(sql)
        self.db_mssql.commit()
        return True

    def account_rewardPoint(self, data_in: dict) -> bool:
        base_sql = "update [RewardPointDB].[dbo].[RewardPointsDetail] set IsAccounted=1 where {} in "
        cur = self.db_mssql.cursor()
        err_flag = False
        if not isEmpty(data_in.get("RewardPointsdetailID")):
            sql = base_sql.format("RewardPointsdetailID") + "(" + data_in.get("RewardPointsdetailID") + ")"
            cur.execute(sql)
            self.db_mssql.commit()

        elif not isEmpty(data_in.get("jobid")):
            sql = base_sql.format("JobId") + "(" + data_in.get("jobid") + ")"
            cur.execute(sql)
            self.db_mssql.commit()
        else:
            err_flag = True
        return err_flag

    def query_goods(self, data_in: dict) -> (int, pd.DataFrame):
        return self._base_query_goods(data_in=data_in)

    def export_goods(self, data_in: dict) -> str:
        _, res_df = self._base_query_goods(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=data_in.get("Operator"))

    def import_goods(self, data_in: dict, file_df: pd.DataFrame) -> bool:
        cur = self.db_mssql.cursor()
        # 先检查GoodsCode存不存在,不存在就新建一个
        all_goodsCode = pd.read_sql(sql="select GoodsCode from Goods where DataStatus=0", con=self.db_mssql)[
            'GoodsCode'].tolist()
        insert_item = []
        base_insert_sql = "insert into RewardPointDB.dbo.Goods(GoodsCode,Name,PointCost,MarketPrice,PurchasePrice,MeasureUnit,CreatedBy) values "
        exist_goodsCode = file_df['商品编码'].drop_duplicates().tolist()
        for _code in exist_goodsCode:
            if _code not in all_goodsCode:
                _item = f'''
({_code},'{file_df.loc[file_df['商品编码'] == _code, '商品名称'].values[0]}',
{file_df.loc[file_df['商品编码'] == _code, '商品单价'].values[0]},'{file_df.loc[file_df['商品编码'] == _code, '市场价'].values[0]}',
'{file_df.loc[file_df['商品编码'] == _code, '购买价'].values[0]}','{file_df.loc[file_df['商品编码'] == _code, '商品计量单位'].values[0]}','{data_in.get('Operator')}')'''
                insert_item.append(_item)
        if insert_item != []:
            insert_sql = base_insert_sql + ','.join(insert_item)
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
{file_df.loc[_index, '数量']},'{file_df.loc[_index, '商品计量单位']}','{data_in.get('Operator')}')'''
            insert_item.append(_item)
        insert_sql = base_insert_sql + ','.join(insert_item)
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

    def query_order(self, data_in: dict) -> (int, pd.DataFrame):
        return self._base_query_order(data_in=data_in)

    def query_orderDetail(self, data_in: dict) -> pd.DataFrame:
        return self._base_query_orderDetail(data_in)

    def query_FixedPoints(self, data_in: dict) -> (int, pd.DataFrame):
        return self._base_query_FixedPoints(data_in)

    def export_FixedPoints(self, data_in: dict) -> str:
        _, res_df = self._base_query_FixedPoints(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=data_in.get("Operator"))

    def query_FixedPoints_ByYear(self, data_in: dict) -> int:
        return self._base_query_FixedPoints_ByYear(data_in=data_in)


if __name__ == "__main__":
    from config.dbconfig import mssqldb, ncdb

    worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
    data = {'page': 6, "pageSize": 10}
    # data = {'jobid': 100055}
    res = worker.query_rewardPoint(data_in=data)
    res2 = worker.query_FixedPointDetail(data_in=data)
    res3 = worker.query_RewardPointSummary(data_in=data)
    res4 = worker._base_query_FixedPoints(data_in=data)
    # res5=worker.query_goods(data_in={"GoodsCode":100005})
    print(res2,res3,res4)
