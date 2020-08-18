# coding:utf-8
'''
接口主体
'''

from baseInterface import BaseRewardPointInterface
from tool.tool import *


class RewardPointInterface(BaseRewardPointInterface):
    def __init__(self, dict mssqlDbInfo, dict ncDbInfo):
        # 数据库连接体
        super(RewardPointInterface, self).__init__(mssqlDbInfo, ncDbInfo)
        mssql_con = self.mssql_pool.connection()
        self.rewardPointChildType = getChlidType(mssql_con)
        self.rewardPointStandard = pd.read_sql(
            sql='''SELECT [RewardPointsStandardID],[RewardPointsTypeID],[CheckItem],[PointsAmount],[ChangeCycle] FROM [RewardPointDB].[dbo].[RewardPointsStandard]''',
            con=mssql_con)  # 积分标准表
        self.HighSchool = pd.read_sql(
            "SELECT [HighSchoolID],[Name],[SchoolProject] FROM [RewardPointDB].[dbo].[HighSchool]",
            con=mssql_con)  # 985/211工程表
        self.HighSchoolList = self.HighSchool['Name'].tolist()
        self.jobrank_count_time = datetime.datetime(year=2018, month=8, day=1)
        self.jobrank_begin_time = datetime.datetime(year=2018, month=1, day=1)
        self.serving_count_time = datetime.datetime(year=2004, month=1, day=1)

    def _set_rewardPointChildType(self):
        '''
        用于刷新内存中的所有积分类型的子类型
        :return:
        '''
        self.rewardPointChildType = getChlidType(dbcon=self.mssql_pool.connection())

    def _set_rewardPointStandard(self):
        self.rewardPointStandard = pd.read_sql(
            sql="select RewardPointsStandardID,RewardPointsTypeID,CheckItem,PointsAmount,ChangeCycle from [RewardPointDB].[dbo].[RewardPointsStandard]",
            con=self.mssql_pool.connection())  # 积分标准表

    def _set_HighSchool(self):
        self.HighSchool = pd.read_sql(
            "SELECT [HighSchoolID],[Name],[SchoolProject] FROM [RewardPointDB].[dbo].[HighSchool]",
            con=self.mssql_pool.connection())  # 985/211工程表

    def _base_query_rewardPointDetail(self, dict data_in) -> (int, pd.DataFrame):
        '''
        用于基础积分详情查询，不直接暴露
        :param data_in:
        :return:
        '''
        cdef str totalLength_sql, beginDate, endDate, rewardPointsType, manName, query_sql, base_sql
        cdef list sql_item, query_item, _rewardPointsType_container
        cdef float totalLength
        mssql_con = self.mssql_pool.connection()
        if data_in:  # 不为空则按照条件查询
            totalLength_sql = "select COUNT([RewardPointsdetailID]) as res from [RewardPointDB].[dbo].[RewardPointsDetail] as dt join [RewardPointDB].[dbo].[RewardPointsType] a on dt.RewardPointsTypeID=a.RewardPointsTypeID inner hash join openquery(NC,'select name,code from bd_psndoc where enablestate =2') as NCDB on NCDB.CODE = dt.JobId"
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
            sql_item.append(query_sql)
            if not (data_in.get("page") and data_in.get("pageSize")):  # 不分页
                sql_item.append('')
            else:
                sql_item.append(" offset {0[0]}*({0[1]}-1) rows fetch next {0[0]} rows only".format(
                    [data_in.get("pageSize"), data_in.get("page")]))
            res_df = pd.read_sql(sql=base_sql.format(sql_item), con=mssql_con)
            # 计算总行数
            totalLength = \
                pd.read_sql(sql=totalLength_sql + " where " + ' and '.join(query_item), con=mssql_con).loc[0, 'res']
        else:
            totalLength_sql = "select COUNT([RewardPointsdetailID]) as res from [RewardPointDB].[dbo].[RewardPointsDetail]"
            totalLength = pd.read_sql(sql=totalLength_sql, con=mssql_con).loc[0, 'res']
            base_sql = "select a.Name as 积分类型,dt.RewardPointsdetailID,dt.DepartmentLv1,dt.DepartmentLv2,dt.DepartmentLv3, " \
                       "dt.FunctionalDepartment,NCDB.NAME as Name,dt.Submit,dt.Post,a.Name as 积分类型, " \
                       "dt.ChangeType,dt.BonusPoints,dt.MinusPoints,dt.ChangeAmount,dt.Reason,dt.Proof,dt.ReasonType," \
                       "dt.JobId,dt.AssessmentDate,dt.IsAccounted  " \
                       "from RewardPointDB.dbo.RewardPointsDetail dt " \
                       "join RewardPointDB.dbo.RewardPointsType a on dt.RewardPointsTypeID=a.RewardPointsTypeID " \
                       "inner hash join openquery(NC,'select name,code from bd_psndoc where enablestate =2') as NCDB on NCDB.CODE = dt.JobId " \
                       "where dt.DataStatus=0 and a.DataStatus=0"
            res_df = pd.read_sql(sql=base_sql, con=mssql_con)
        return totalLength, res_df

    def _base_query_RewardPointSummary(self, dict data_in) -> (int, pd.DataFrame):
        # 基本信息
        mssql_con = self.mssql_pool.connection()
        nc_con = self.nc_pool.connection()
        today = datetime.datetime.today()  # 今天
        cdef list query_item = ["hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                                "bd_psndoc.enablestate =2",
                                "bd_psncl.name not in ('独立业务员')",
                                "regexp_like(org_adminorg.name,'(威腾电气|威通|西屋)')"]
        cdef bint notemptyflag, is985211
        cdef str maninfo_base_sql, all_id_tupe, education, schoolname, tittle_rank, tittle_name
        cdef list sql_item, all_id, tempidlist, jobrank_data
        cdef float totalLength, SchoolPoints, TittlePoint, jobrankpoint
        cdef int ServingAgePoints, years, months
        if data_in != {}:
            notemptyflag = 1
        else:
            notemptyflag = 0
        # 是否取离职人员
        if data_in.get('onduty')==0: # 0取在职的
            query_item.append("hi_psnjob.endflag ='N' and hi_psnjob. poststat='Y'")
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
        maninfo_df = pd.read_sql(sql=maninfo_base_sql.format(sql_item), con=nc_con)
        maninfo_df = maninfo_df.reindex(columns=(
            '姓名', '工号', '组织', '部门', '职称等级', '学历', '学校', '固定积分', '年度累计积分', '总累计积分'))
        all_id = maninfo_df['工号'].tolist()
        if len(all_id) == 0:
            return 0, maninfo_df
        tempidlist = []
        for _ii in all_id:
            tempidlist.append("\'" + _ii + "\'")
        all_id_tupe = ','.join(tempidlist)
        # 计算数据长度
        totalLength = super()._get_manlength(con=nc_con, sql_item=sql_item)
        #  学历
        school_df = super()._get_education(con=mssql_con, id=all_id_tupe, notemptyflag=notemptyflag)
        #  职称
        techtittle_df = super()._get_techtittle(con=mssql_con, id=all_id_tupe, notemptyflag=notemptyflag)
        # A 管理分表
        pointdetail = super()._get_A_managePoint(con=mssql_con, id=all_id_tupe, notemptyflag=notemptyflag,
                                                 thisyear=today.year)
        maninfo_df = pd.merge(maninfo_df, pointdetail, left_on='工号', right_on='JobId', how='left').fillna(0)
        #  工龄表
        manServing_df = super()._get_ServingAge(con=nc_con, id=all_id_tupe, notemptyflag=notemptyflag)
        #  职务表
        jobrank_df = super()._get_jobrank(con=mssql_con, all_id=all_id, notemptyflag=notemptyflag)
        # 填充
        for man in all_id:
            man_select = maninfo_df['工号'] == man
            # 学历积分
            SchoolPoints, education, schoolname, is985211 = super()._count_educationPoint(school_df=school_df, man=man,
                                                                                          HighSchoolList=self.HighSchoolList)
            maninfo_df.loc[man_select, '学历积分'] = SchoolPoints
            maninfo_df.loc[man_select, '学历'] = education
            maninfo_df.loc[man_select, '学校'] = schoolname
            # 职称积分
            TittlePoint, tittle_rank, tittle_name = super()._count_tittlePoint(techtittle_df=techtittle_df, man=man)
            maninfo_df.loc[man_select, '职称等级'] = tittle_rank
            # 工龄积分
            ServingAgePoints, serving_begindate, years, months = super()._count_serveragePoint(
                manServing_df=manServing_df, man=man,
                thisyear=today.year,
                serving_count_time=self.serving_count_time)
            # 职务积分
            jobrankpoint, jobrank_data = super()._count_jobrankpoint(jobrank_df=jobrank_df, man=man,
                                                                     jobrank_count_time=self.jobrank_count_time,
                                                                     jobrank_begin_time=self.jobrank_begin_time,
                                                                     thisyear=today.year)
            maninfo_df.loc[man_select, '固定积分'] = SchoolPoints + TittlePoint + ServingAgePoints + jobrankpoint
            maninfo_df.loc[man_select, '总累计积分'] = maninfo_df.loc[man_select, '固定积分'] + maninfo_df.loc[
                man_select, '总获得管理积分']
            maninfo_df.loc[man_select, '年度累计积分'] = maninfo_df.loc[man_select, '固定积分'] + maninfo_df.loc[
                man_select, '年度管理积分']
        return totalLength, maninfo_df

    def _base_query_FixedPoints(self, dict data_in) -> (int, pd.DataFrame):
        today = datetime.datetime.today()
        mssql_con = self.mssql_pool.connection()
        nc_con = self.nc_pool.connection()
        cdef list query_item = ["hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                                "bd_psndoc.enablestate =2",
                                "bd_psncl.name not in ('独立业务员')",
                                "regexp_like(org_adminorg.name,'(威腾电气|威通|西屋)')"]
        cdef bint notemptyflag
        cdef str maninfo_base_sql, maninfo_sql, all_id_tupe, man, education, schoolname, tittle_name
        cdef list sql_item, tempidlist, all_id
        cdef float totalLength, SchoolPoints, TittlePoint, ServingAgePoints, jobrankpoint
        if data_in != {}:
            notemptyflag = 1
        else:
            notemptyflag = 0
        # 是否取离职人员
        if data_in.get('onduty')==0: # 0取在职的
            query_item.append("hi_psnjob.endflag ='N' and hi_psnjob. poststat='Y'")

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
        maninfo_df = pd.read_sql(sql=maninfo_sql, con=nc_con)
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
        # 计算数据长度
        totalLength = super()._get_manlength(con=nc_con, sql_item=sql_item)
        #  学历
        school_df = super()._get_education(con=mssql_con, id=all_id_tupe, notemptyflag=notemptyflag)
        #  职称
        techtittle_df = super()._get_techtittle(con=mssql_con, id=all_id_tupe, notemptyflag=notemptyflag)
        #  工龄表
        manServing_df = super()._get_ServingAge(con=nc_con, id=all_id_tupe, notemptyflag=notemptyflag)
        #  职务表
        jobrank_df = super()._get_jobrank(con=mssql_con, all_id=all_id, notemptyflag=notemptyflag)
        for _index in res_df.index:
            man = res_df.loc[_index, '工号']
            man_select = maninfo_df['工号'] == man
            for _item in ['姓名', '部门', '职务', '组织']:
                if len(maninfo_df.loc[man_select, _item]) > 0:
                    res_df.loc[_index, _item] = maninfo_df.loc[man_select, _item].values[0]
            #  学历积分
            SchoolPoints, education, schoolname, _ = super()._count_educationPoint(school_df=school_df, man=man,
                                                                                   HighSchoolList=self.HighSchoolList)
            res_df.loc[_index, '学历积分'] = SchoolPoints
            res_df.loc[_index, '学历'] = education
            # 职称积分
            TittlePoint, _, tittle_name = super()._count_tittlePoint(techtittle_df=techtittle_df, man=man)
            res_df.loc[_index, '职称积分'] = TittlePoint
            res_df.loc[_index, '职称'] = tittle_name
            # 工龄积分
            ServingAgePoints, _, _, _ = super()._count_serveragePoint(manServing_df=manServing_df, man=man,
                                                                      thisyear=today.year,
                                                                      serving_count_time=self.serving_count_time)
            res_df.loc[_index, '入职时间'] = manServing_df.loc[manServing_df['CODE'] == man, 'BEGINDATE'].values[0]
            res_df.loc[_index, '工龄积分'] = ServingAgePoints
            # 职务积分
            jobrankpoint, _ = super()._count_jobrankpoint(jobrank_df=jobrank_df, man=man,
                                                          jobrank_count_time=self.jobrank_count_time,
                                                          jobrank_begin_time=self.jobrank_begin_time,
                                                          thisyear=today.year)
            res_df.loc[_index, '职务积分'] = jobrankpoint
        return totalLength, res_df

    def _base_query_goods(self, dict data_in) -> (int, pd.DataFrame):
        cdef list query_item, sql_item
        cdef str goodName, query_sql, base_sql, totalLength_sql
        cdef float totalLength
        mssql_con = self.mssql_pool.connection()
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
                sql_item.append(" offset {0[0]}*({0[1]}-1) rows fetch next {0[0]} rows only".format(
                    [data_in.get("pageSize"), data_in.get("page")]))
            # 拼起来
            res_df = pd.read_sql(sql=base_sql.format(sql_item), con=mssql_con)
            # 计算总行数
            totalLength_sql = "select count(GoodsID) as res from [RewardPointDB].[dbo].[Goods] "
            lensql = totalLength_sql + " where " + ' and '.join(query_item)
            totalLength = pd.read_sql(sql=lensql, con=mssql_con).loc[0, 'res']
        else:
            base_sql = "select * from [RewardPointDB].[dbo].[Goods] where DataStatus=0"
            res_df = pd.read_sql(sql=base_sql, con=mssql_con)
            # 计算总行数
            totalLength_sql = "select count(GoodsID) as res from [RewardPointDB].[dbo].[Goods] "
            totalLength = pd.read_sql(sql=totalLength_sql, con=mssql_con).loc[0, 'res']
        return totalLength, res_df

    def _base_query_order(self, dict data_in) -> (int, pd.DataFrame):
        cdef list query_item, sql_item
        cdef str query_sql, base_sql, totalLength_sql
        cdef float totalLength
        mssql_con = self.mssql_pool.connection()
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
            sql_item = [query_sql, " offset {0[0]}*({0[1]}-1) rows fetch next {0[0]} rows only".format(
                [data_in.get("pageSize"), data_in.get("page")])]
        # 拼起来
        sql = base_sql.format(sql_item)
        res_df = pd.read_sql(sql=sql, con=mssql_con)
        totalLength_sql = "select count(PointOrderID) as res from [RewardPointDB].[dbo].[PointOrder] {0[0]}".format(
            sql_item)
        totalLength = pd.read_sql(sql=totalLength_sql, con=mssql_con).loc[0, 'res']
        return totalLength, res_df

    def _base_query_orderDetail(self, dict data_in) -> pd.DataFrame:
        cdef list query_item, sql_item
        cdef str query_sql, base_sql, totalLength_sql, orderID
        cdef float totalLength
        mssql_con = self.mssql_pool.connection()
        query_item = ['ordergoods.DataStatus=0']
        orderID = data_in.get('PointOrderID')
        query_item.append(f"orders.PointOrderID in ({orderID})")
        base_sql = '''
        select orders.PointOrderID,goods.PictureUrl,goods.GoodsCode,goods.Status,goods.Name,goods.PointCost,
        ordergoods.OrderGoodsAmount
        from RewardPointDB.dbo.PointOrderGoods as ordergoods 
        join RewardPointDB.dbo.PointOrder as orders on orders.PointOrderID=ordergoods.PointOrderID
        join RewardPointDB.dbo.Goods as goods on goods.GoodsID=ordergoods.GoodsID {}'''
        orderDetail_df = pd.read_sql(base_sql.format(" where " + " and ".join(query_item)), con=mssql_con)
        return orderDetail_df

    def _base_query_FixedPoints_ByYear(self, dict data_in) -> int:
        cdef str sql = f"select sum(dt.BonusPoints)-sum(dt.MinusPoints) as AnnualSum from RewardPointDB.dbo.RewardPointsDetail dt where dt.DataStatus=0 and dt.RewardPointsTypeID =3 and dt.AssessmentDate>'{data_in.get('year')}-01-01 00:00:00' and dt.AssessmentDate<'{data_in.get('year') + 1}-01-01 00:00:00' and dt.JobId='{data_in.get('jobid')}'"
        res_df = pd.read_sql(sql=sql, con=self.mssql_pool.connection())
        return res_df.loc[0, 'AnnualSum']

    def _base_query_FixedPointDetail(self, dict data_in) -> dict:
        mssql_con = self.mssql_pool.connection()
        nc_con = self.nc_pool.connection()
        today = datetime.datetime.today()  # 今天
        cdef str man, maninfo_base_sql, education, schoolname, tittle_rank, tittle_name
        cdef list query_item, jobrank_data
        cdef float totalLength, SchoolPoints, TittlePoint, jobrankpoint, ServingAgePoints
        cdef bint is985211
        cdef int years, months
        man = data_in.get('jobid')
        man_data = {}
        man_data['工号'] = data_in.get('jobid')
        maninfo_base_sql = "select bd_psndoc.name as 姓名,bd_psndoc.code as 工号 from hi_psnjob join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc"
        query_item = ["hi_psnjob.endflag ='N'", "hi_psnjob.ismainjob ='Y'", "hi_psnjob.lastflag  ='Y'",
                      "bd_psndoc.enablestate =2", f"bd_psndoc.code='{man}'"]
        maninfo_df = pd.read_sql(sql=maninfo_base_sql + " where " + ' and '.join(query_item), con=nc_con)
        if len(maninfo_df) == 0:
            return man_data
        #  学历
        school_df = super()._get_education(con=mssql_con, id="\'" + man + "\'", notemptyflag=True)
        #  职称
        techtittle_df = super()._get_techtittle(con=mssql_con, id="\'" + man + "\'", notemptyflag=True)
        #  职务
        jobrank_df = super()._get_jobrank(con=mssql_con, all_id=[man], notemptyflag=True)
        #  工龄
        manServing_df = super()._get_ServingAge(con=nc_con, id="\'" + man + "\'", notemptyflag=True)
        #  学历积分
        School_data, serving_data = {}, {}
        SchoolPoints, education, schoolname, is985211 = super()._count_educationPoint(school_df=school_df,
                                                                                      man=data_in.get('jobid'),
                                                                                      HighSchoolList=self.HighSchoolList)
        School_data['schoolPoints'] = SchoolPoints
        School_data['education'] = education
        School_data['is985211'] = is985211
        # 职称积分
        Tittle_data = {}
        TittlePoint, tittle_rank, tittle_name = super()._count_tittlePoint(techtittle_df=techtittle_df,
                                                                           man=data_in.get('jobid'))
        Tittle_data['tittleRankPoint'] = TittlePoint
        Tittle_data['tectittle'] = tittle_name
        Tittle_data['tittleRank'] = tittle_rank
        #  工龄积分
        ServingAgePoints, _, years, months = super()._count_serveragePoint(manServing_df=manServing_df,
                                                                           man=data_in.get('jobid'),
                                                                           thisyear=today.year,
                                                                           serving_count_time=self.serving_count_time)
        serving_data['begindate'] = manServing_df.loc[0, 'BEGINDATE']
        serving_data['servingAgePoints'] = ServingAgePoints
        serving_data['servingAge_years'] = years
        serving_data['servingAge_months'] = months
        # 职务积分
        _, jobrank_data = super()._count_jobrankpoint(jobrank_df=jobrank_df, man=data_in.get('jobid'),
                                                      jobrank_count_time=self.jobrank_count_time,
                                                      jobrank_begin_time=self.jobrank_begin_time, thisyear=today.year)

        # 存起来
        man_data['学历积分'] = School_data
        man_data['职称积分'] = Tittle_data
        man_data['工龄积分'] = serving_data
        man_data['职务积分'] = jobrank_data

        return man_data

    def query_RewardPointSummary(self, dict data_in) -> (int, pd.DataFrame):
        return self._base_query_RewardPointSummary(data_in)

    def export_RewardPointSummary(self, dict data_in) -> str:
        Operator = data_in.pop("Operator", 404)
        _, res_df = self._base_query_RewardPointSummary(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=Operator)

    def query_FixedPointDetail(self, dict data_in) -> dict:
        return self._base_query_FixedPointDetail(data_in=data_in)

    def query_rewardPoint(self, dict data_in) -> (int, pd.DataFrame):
        '''
        返回积分详情查询信息
        :param data_in:
        :return:
        '''
        return self._base_query_rewardPointDetail(data_in=data_in)

    def delete_rewardPoint(self, dict data_in) -> bool:
        mssql_con = self.mssql_pool.connection()
        cdef str base_sql = "update [RewardPointDB].[dbo].[RewardPointsDetail] set DataStatus=1 where RewardPointsdetailID = {}"
        cdef str sql = base_sql.format(data_in.get("RewardPointsdetailID"))
        cur = mssql_con.cursor()
        cur.execute(sql)
        mssql_con.commit()
        return True

    def export_rewardPoint(self, dict data_in) -> str:
        '''
        包装积分详情信息，生成EXCEL并返回下载链接
        :param data_in:
        :return:
        '''
        Operator = data_in.pop("Operator", 404)
        _, res_df = self._base_query_rewardPointDetail(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=Operator)

    def import_rewardPoint_onesql(self, dict data_in, file_df: pd.DataFrame) -> bool:
        mssql_con = self.mssql_pool.connection()
        rewardPointType_df = pd.read_sql(
            "select RewardPointsTypeID,Name from RewardPointDB.dbo.RewardPointsType where DataStatus=0",
            con=mssql_con)
        cdef str base_sql = '''
        insert into [RewardPointDB].[dbo].[RewardPointsDetail] (
         ChangeType,CreatedBy,JobId,DepartmentLv1,DepartmentLv2,DepartmentLv3,Post,Name,RewardPointsTypeID,BonusPoints,
         MinusPoints,Reason,Proof,ReasonType,FunctionalDepartment,Submit,AssessmentDate ) values{}
        '''
        cdef list sql_values = []
        cdef list value_check = ['工号', '一级部门', '二级部门', '三级部门', '职务名称',
                                 '姓名', 'A分/B分', '加分',
                                 '减分', '加减分理由', '加减分依据', '理由分类', '职能部门/所在部门管理', '提交部门',
                                 '考核日期']
        cdef list values_item
        cdef float RewardPointsTypeID
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
        cdef str sql = base_sql.format(','.join(sql_values))
        cur = mssql_con.cursor()
        try:
            cur.execute(sql)
            mssql_con.commit()
            return True
        except Exception as e:
            return False

    @verison_warning
    def import_rewardPoint(self, dict data_in, file_df: pd.DataFrame) -> bool:
        mssql_con = self.mssql_pool.connection()
        cdef list sql_item, sql_values
        cdef str sql
        rewardPointType_df = pd.read_sql(
            "select RewardPointsTypeID,Name from RewardPointDB.dbo.RewardPointsType where DataStatus=0",
            con=mssql_con)
        cdef str base_sql = "insert into [RewardPointDB].[dbo].[RewardPointsDetail] ( {} ) values ( {} )"
        cur = mssql_con.cursor()
        cdef dict column_dic = {'工号': 'JobId', '一级部门': "DepartmentLv1", '二级部门': "DepartmentLv2",
                                '三级部门': "DepartmentLv3",
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
        mssql_con.commit()
        return True

    def account_rewardPoint(self, dict data_in) -> bool:
        mssql_con = self.mssql_pool.connection()
        cdef str base_sql = "update [RewardPointDB].[dbo].[RewardPointsDetail] set IsAccounted=1 where {} in "
        cur = mssql_con.cursor()
        cdef bint err_flag = 0
        cdef str sql
        if not isEmpty(data_in.get("RewardPointsdetailID")):
            sql = base_sql.format("RewardPointsdetailID") + "(" + data_in.get("RewardPointsdetailID") + ")"
            cur.execute(sql)
            mssql_con.commit()
        elif not isEmpty(data_in.get("jobid")):
            sql = base_sql.format("JobId") + "(" + data_in.get("jobid") + ")"
            cur.execute(sql)
            mssql_con.commit()
        else:
            err_flag = 1
        return err_flag

    def query_goods(self, dict data_in) -> (int, pd.DataFrame):
        return self._base_query_goods(data_in=data_in)

    def export_goods(self, dict data_in) -> str:
        Operator = data_in.pop("Operator", 404)
        _, res_df = self._base_query_goods(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=Operator)

    def import_goods(self, dict data_in, file_df: pd.DataFrame) -> bool:
        mssql_con = self.mssql_pool.connection()
        cur = mssql_con.cursor()
        # 先检查GoodsCode存不存在,不存在就新建一个
        all_goodsCode = pd.read_sql(sql="select GoodsCode from Goods where DataStatus=0", con=mssql_con)[
            'GoodsCode'].tolist()
        cdef list insert_item = []
        cdef str base_insert_sql = "insert into RewardPointDB.dbo.Goods(GoodsCode,Name,PointCost,MarketPrice,PurchasePrice,MeasureUnit,CreatedBy) values "
        cdef list exist_goodsCode = file_df['商品编码'].drop_duplicates().tolist()
        for _code in exist_goodsCode:
            if _code not in all_goodsCode:
                _item = f'''
({_code},'{file_df.loc[file_df['商品编码'] == _code, '商品名称'].values[0]}',
{file_df.loc[file_df['商品编码'] == _code, '商品单价'].values[0]},'{file_df.loc[file_df['商品编码'] == _code, '市场价'].values[0]}',
'{file_df.loc[file_df['商品编码'] == _code, '购买价'].values[0]}','{file_df.loc[file_df['商品编码'] == _code, '商品计量单位'].values[0]}','{data_in.get('Operator')}')'''
                insert_item.append(_item)
        if insert_item != []:
            cur.execute(base_insert_sql + ','.join(insert_item))
        # 提交
        mssql_con.commit()
        # 变量清空,复用,开始插入库存
        insert_item.clear()
        base_insert_sql = "insert into RewardPointDB.dbo.StockInDetail(" \
                          "GoodsID,ChangeType,ChangeAmount,MeasureUnit,CreatedBy) values "
        all_goodsCode = pd.read_sql("select GoodsID,GoodsCode from Goods where DataStatus=0", mssql_con)
        for _index in file_df.index:
            insert_item.append(f'''
({all_goodsCode.loc[all_goodsCode['GoodsCode'] == file_df.loc[_index, '商品编码'], 'GoodsID'].values[0]},0,
{file_df.loc[_index, '数量']},'{file_df.loc[_index, '商品计量单位']}','{data_in.get('Operator')}')''')
        cur.execute(base_insert_sql + ','.join(insert_item))
        # 提交
        mssql_con.commit()
        return True

    def set_goods_status(self, dict data_in) -> bool:
        mssql_con = self.mssql_pool.connection()
        cdef str sql = f"update [RewardPointDB].[dbo].[Goods] set Status={data_in.get('Status')} where GoodsCode in ({data_in.get('GoodsCode')})"
        cur = mssql_con.cursor()
        cur.execute(sql)
        mssql_con.commit()
        return True

    def upload_goodsImage(self, dict data_in, str image_url) -> bool:
        mssql_con = self.mssql_pool.connection()
        cur = mssql_con.cursor()
        cdef str base_sql = "update RewardPointDB.dbo.Goods set PictureUrl={}"
        cdef list query_item = []
        query_item.append(f"GoodsCode = {data_in.get('GoodsCode')}")
        cur.execute(base_sql.format("\'" + image_url + "\'") + " where " + ' and '.join(query_item))
        mssql_con.commit()
        return True

    def query_order(self, dict data_in) -> (int, pd.DataFrame):
        return self._base_query_order(data_in=data_in)

    def query_orderDetail(self, dict data_in) -> pd.DataFrame:
        return self._base_query_orderDetail(data_in)

    def query_FixedPoints(self, dict data_in) -> (int, pd.DataFrame):
        return self._base_query_FixedPoints(data_in)

    def export_FixedPoints(self, dict data_in) -> str:
        Operator = data_in.pop("Operator", 404)
        _, res_df = self._base_query_FixedPoints(data_in=data_in)
        return get_dfUrl(df=res_df, Operator=Operator)

    def query_FixedPoints_ByYear(self, dict data_in) -> int:
        return self._base_query_FixedPoints_ByYear(data_in=data_in)
