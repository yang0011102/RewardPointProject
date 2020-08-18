# utf-8
import pymssql, cx_Oracle, datetime
from tool import *
from DBUtils.PooledDB import PooledDB
from pandas import DataFrame, read_sql, isna

class BaseRewardPointInterface:
    def __init__(self, dict mssqlDbInfo, dict ncDbInfo):
        # 数据库连接体
        self.mssql_pool = PooledDB(creator=pymssql, mincached=4, maxcached=8, maxshared=4, maxconnections=10,
                                   blocking=True, **mssqlDbInfo)
        self.nc_pool = PooledDB(creator=cx_Oracle, mincached=4, maxcached=8, maxshared=4, maxconnections=10,
                                blocking=True,
                                user=ncDbInfo.get('user'), password=ncDbInfo.get('password'),
                                dsn=f"{ncDbInfo['host']}:{ncDbInfo['port']}/{ncDbInfo['db']}",
                                encoding="UTF-8", nencoding="UTF-8")
        # self.db_mssql = pymssql.connect(**mssqlDbInfo)
        # self.db_nc = cx_Oracle.connect(
        #     f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
        #     encoding="UTF-8", nencoding="UTF-8")

    def _get_education(self, con: pymssql.Connection, str id, bint notemptyflag) -> DataFrame:
        cdef str base_sql = "select ncinfo.jobid,ncinfo.educationname,ncinfo.schoolname,std.PointsAmount from openquery(NC,'select bd_psndoc.code as jobid,c1.name as educationname,edu.school as schoolname from bd_psndoc left join hi_psndoc_edu edu on bd_psndoc.pk_psndoc = edu.pk_psndoc left join bd_defdoc c1 on edu.education = c1.pk_defdoc where bd_psndoc.enablestate =2') as ncinfo left hash join RewardPointDB.dbo.RewardPointsStandard as std on std.CheckItem=ncinfo.educationname"
        cdef list query_item = ["(std.RewardPointsTypeID=7 or std.RewardPointsTypeID is null)"]
        if notemptyflag:
            query_item.append(f"ncinfo.jobid in ({id})")
        school_df = read_sql(sql=base_sql + " where " + ' and '.join(query_item), con=con).fillna(
            {"schoolname": '', "PointsAmount": 0})
        return school_df

    def _get_techtittle(self, con: pymssql.Connection, str id, bint notemptyflag) -> DataFrame:
        cdef str base_sql = "select ncinfo.jobid,ncinfo.tectittlename,ncinfo.tectittlerank,std.PointsAmount from openquery(NC,'select bd_psndoc.code as jobid,tectittle.name as tectittlename,tittlerank.name as tectittlerank from bd_psndoc left join hi_psndoc_title on bd_psndoc.pk_psndoc = hi_psndoc_title.pk_psndoc  left join bd_defdoc tectittle on hi_psndoc_title.pk_techposttitle = tectittle.pk_defdoc left join bd_defdoc tittlerank on hi_psndoc_title.titlerank =tittlerank.pk_defdoc where bd_psndoc.enablestate =2') as ncinfo left hash join RewardPointDB.dbo.RewardPointsStandard as std on std.CheckItem=ncinfo.tectittlerank"
        cdef list query_item = ["(std.RewardPointsTypeID=6 or std.RewardPointsTypeID is null)"]
        if notemptyflag:
            query_item.append(f"ncinfo.jobid in ({id})")
        techtittle_df = read_sql(sql=base_sql + " where " + ' and '.join(query_item), con=con).fillna(
            {"tectittlename": '', "tectittlerank": '', 'PointsAmount': 0})
        return techtittle_df

    def _get_A_managePoint(self, con: pymssql.Connection, str id, bint notemptyflag, int thisyear) -> DataFrame:
        cdef str mssql_base_sql = "select dt.*,dt.总可用管理积分-isnull(od.pointuse,0) as 现有管理积分 from (select dt.JobId,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.IsAccounted=0 then dt.BonusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.IsAccounted=0 then dt.MinusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 and dt.IsAccounted=1 then dt.ChangeAmount else 0 end) as 现有A分,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.BonusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.MinusPoints else 0 end) as 总可用管理积分,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 and dt.AssessmentDate>{0[0]} then dt.BonusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 and dt.AssessmentDate>{0[0]} then dt.MinusPoints else 0 end) as 年度管理积分,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=1 then dt.BonusPoints else 0 end) as 总获得A分,sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.BonusPoints else 0 end)-sum(case when dt.DataStatus=0 and dt.RewardPointsTypeID=3 and dt.ChangeType=0 then dt.MinusPoints else 0 end) as 总获得管理积分 from RewardPointDB.dbo.RewardPointsDetail dt where dt.DataStatus=0 group by dt.JobId) as dt left join (select JobId,sum(case when od.DataStatus=0 and od.OrderStatus in (0,1,2) then od.TotalPrice else 0 end) as pointuse from RewardPointDB.dbo.PointOrder od group by JobId) od on od.JobId=dt.JobId "
        cdef list sql_item = ["\'" + datetime_string(datetime.datetime(year=thisyear - 1, month=12, day=31)) + "\'", ]
        if notemptyflag:
            query_item = [f"where dt.JobId in ({id})"]
        else:
            query_item = ['']
        sql_item.append(' and '.join(query_item))
        mssql_sql = mssql_base_sql.format(sql_item)
        pointdetail = read_sql(mssql_sql, con).fillna(
            {"现有管理积分": 0, "现有A分": 0, "总可用管理积分": 0, "年度管理积分": 0, "总获得A分": 0, "总获得管理积分": 0, "pointuse": 0})
        return pointdetail

    def _get_ServingAge(self, con: cx_Oracle.Connection, str id, bint notemptyflag) -> DataFrame:
        cdef str ServingAge_base_sql = "select bd_psndoc.code,min(hi_psndoc_psnchg.begindate) as begindate from hi_psndoc_psnchg join bd_psndoc on hi_psndoc_psnchg.pk_psndoc=bd_psndoc.pk_psndoc where (hi_psndoc_psnchg.enddate>'2004-01-01' or hi_psndoc_psnchg.enddate is null) {} group by bd_psndoc.code "
        if notemptyflag:
            ServingAge_sql = ServingAge_base_sql.format(f"and bd_psndoc.code in ({id})")
        else:
            ServingAge_sql = ServingAge_base_sql.format("")
        manServing_df = read_sql(ServingAge_sql, con)
        return manServing_df

    def _get_jobrank(self, con: pymssql.Connection, list all_id, bint notemptyflag) -> DataFrame:
        cdef str jobrank_base_sql = "select ncinfo.*,std.PointsAmount from openquery(NC,'select hi_psnjob.begindate,hi_psnjob.enddate,bd_psndoc.code,om_job.jobname,om_jobrank.jobrankname as 职等 from hi_psnjob join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc join om_jobrank on om_jobrank.pk_jobrank = hi_psnjob.pk_jobrank join om_job on om_job.pk_job=hi_psnjob.pk_job where hi_psnjob.ismainjob =''Y'' and bd_psndoc.enablestate =2 and (hi_psnjob.enddate>''2018-01-01'' or hi_psnjob.enddate is null) {} order by bd_psndoc.code,hi_psnjob.begindate') as ncinfo inner hash join RewardPointDB.dbo.RewardPointsStandard  as std on ncinfo.职等=std.CheckItem where std.DataStatus=0"
        cdef list tempidlist = []
        cdef str all_id_tupe = ""
        cdef str jobrank_sql
        for _ii in all_id:
            tempidlist.append("\'\'" + _ii + "\'\'")
            all_id_tupe = ','.join(tempidlist)
        if notemptyflag:
            jobrank_sql = jobrank_base_sql.format(f"and bd_psndoc.code in ({all_id_tupe})")
        else:
            jobrank_sql = jobrank_base_sql.format("")
        jobrank_df = read_sql(jobrank_sql, con).fillna({"PointsAmount": 0, "JOBNAME": "", "职等": ""})
        return jobrank_df

    def _get_manlength(self, con: cx_Oracle.Connection, list sql_item):
        cdef str length_base_sql = "select count(rownum) as res from hi_psnjob left join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc left join org_adminorg on org_adminorg.pk_adminorg  =hi_psnjob.pk_org left join bd_psncl on bd_psncl.pk_psncl=hi_psnjob. pk_psncl {0[0]}"
        cdef float totalLength = read_sql(sql=length_base_sql.format(sql_item), con=con).loc[0, 'RES']
        return totalLength

    def _count_educationPoint(self, school_df: DataFrame, str man, list HighSchoolList):
        cdef float SchoolPoints = 0
        cdef str schoolname = ''
        cdef str education = ''
        cdef bint is985211 = 0
        if len(school_df.loc[school_df['jobid'] == man, :]) > 0:
            for _edu, _name, _p in zip(school_df.loc[school_df['jobid'] == man, "educationname"],
                                       school_df.loc[school_df['jobid'] == man, "schoolname"],
                                       school_df.loc[school_df['jobid'] == man, "PointsAmount"]):

                if isna(_p): _p = 0
                if _edu == '本科' and not isna(_name):
                    if _name in HighSchoolList:
                        _p += 500
                        is985211 = 1
                if _p > SchoolPoints:
                    SchoolPoints = _p
                    schoolname = _name
                    education = _edu
        return SchoolPoints, education, schoolname, is985211

    def _count_tittlePoint(self, techtittle_df: DataFrame, str man):
        cdef float TittlePoint = 0
        cdef str tittle_rank = ''
        cdef str tittle_name = ''
        if len(techtittle_df.loc[techtittle_df['jobid'] == man, :]) > 0:
            for _tn, _tr, _p in zip(techtittle_df.loc[techtittle_df['jobid'] == man, 'tectittlename'],
                                    techtittle_df.loc[techtittle_df['jobid'] == man, 'tectittlerank'],
                                    techtittle_df.loc[techtittle_df['jobid'] == man, 'PointsAmount']):
                if isna(_p): _p = 0
                if _p > TittlePoint:
                    TittlePoint = _p
                    tittle_rank = _tr
                    tittle_name = _tn
        return TittlePoint, tittle_rank, tittle_name

    def _count_serveragePoint(self, manServing_df: DataFrame, serving_count_time: datetime.datetime, str man,
                              int thisyear):
        cdef int years, months, ServingAgePoints
        serving_begindate = datetime.datetime.strptime(
            manServing_df.loc[manServing_df['CODE'] == man, 'BEGINDATE'].values[0], "%Y-%m-%d")  # 取起始时间
        if serving_begindate.__le__(serving_count_time):  # 如果早于serving_count_time,那么从serving_count_time开始算
            serving_begindate = serving_count_time
        if serving_begindate.__lt__(datetime.datetime(year=2018, month=1, day=1)):
            if serving_begindate.__lt__(datetime.datetime(year=serving_begindate.year, month=8, day=1)):  # 如果早于当年8.1
                serving_begindate = datetime.datetime(year=serving_begindate.year, month=1, day=1)  # 从当年元旦开始计算
            else:
                serving_begindate = datetime.datetime(year=serving_begindate.year + 1, month=1, day=1)  # 从下年元旦开始计算
        years, months = countTime_NewYear(beginDate=serving_begindate,
                                          endDate=datetime.datetime(year=thisyear, month=12, day=30))
        ServingAgePoints = np.around((years + months / 12) * 2000)
        return ServingAgePoints, serving_begindate, years, months

    def _count_jobrankpoint(self, jobrank_df: DataFrame, str man, int thisyear,
                            jobrank_count_time: datetime.datetime,
                            jobrank_begin_time: datetime.datetime):
        cdef float jobrankpoint = 0
        cdef list jobrank_data = []  # 职务积分详情容器
        cdef bint islatest
        cdef int months
        cdef float temp_jobrankpoint
        man_workinfo = jobrank_df.loc[jobrank_df['CODE'] == man, :].reset_index()  # 取个人的工作信息
        if len(man_workinfo) > 0:
            jobrank_begindate = datetime.datetime.strptime(man_workinfo.loc[0, 'BEGINDATE'], "%Y-%m-%d")  # 取起始时间
            if jobrank_begindate.__le__(jobrank_count_time):  # 如果早于jobrank_count_time,那么从jobrank_begin_time开始算
                jobrank_begindate = jobrank_begin_time
            man_workinfo.loc[0, 'BEGINDATE'] = jobrank_begindate  # 填回去
            for work_index in man_workinfo.index:
                if isna(man_workinfo.loc[work_index, 'ENDDATE']):
                    temp_enddate = datetime.datetime(year=thisyear, month=12, day=31)
                    islatest = True
                else:
                    temp_enddate = datetime.datetime.strptime(man_workinfo.loc[work_index, 'ENDDATE'], "%Y-%m-%d")
                    islatest = False
                if isinstance(man_workinfo.loc[work_index, 'BEGINDATE'], str):
                    temp_begindate = datetime.datetime.strptime(man_workinfo.loc[work_index, 'BEGINDATE'],
                                                                "%Y-%m-%d")
                else:
                    temp_begindate = man_workinfo.loc[work_index, 'BEGINDATE']

                months = sub_datetime_Bydayone(beginDate=temp_begindate, endDate=temp_enddate)
                temp_jobrankpoint = np.around(man_workinfo.loc[work_index, 'PointsAmount'] * months / 12)
                jobrankpoint += temp_jobrankpoint
                jobrank_data.append({'begindate': datetime_string(temp_begindate, timeType="%Y-%m-%d"),
                                     'enddate': datetime_string(temp_enddate, timeType="%Y-%m-%d"),
                                     'islatest': islatest,
                                     'jobrank': man_workinfo.loc[work_index, '职等'],
                                     'jobname': man_workinfo.loc[work_index, 'JOBNAME'],
                                     'jobrankpoint': temp_jobrankpoint})

        return jobrankpoint, jobrank_data
