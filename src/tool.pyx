# utf-8
import datetime
import functools
import json
import time
import pymssql
from numpy import isnan,integer,floating,ndarray
from pandas import DataFrame, read_sql, isna, Timestamp, to_datetime
from warnings import warn
from config import *
import simplejson

def verison_warning(func):
    @functools.wraps(func)
    def __warning(*args, **kwargs):
        warn(
            "This function is outdated. Get in touch with the administrator for information about the new version",
            DeprecationWarning)
        return func(*args, **kwargs)

    return __warning

cpdef list df_tolist(df: DataFrame):
    cdef list res_list = []
    for _index in df.index.tolist():
        res_list.append(df.loc[_index, :].to_dict())
    return res_list

cpdef str datetime_string(t: datetime.datetime, timeType="%Y-%m-%d %H:%M:%S"):
    return t.strftime(timeType)

def str2timestamp(str timestring, timeType="%Y-%m-%d %H:%M:%S"):
    return time.mktime(time.strptime(timestring, timeType))

cpdef bint time_compare(str time1, str time2, str timeType):
    return str2timestamp(time1, timeType) > str2timestamp(time2, timeType)

cpdef float sub_stringtime(time1: str, time2: str, timeType):
    return str2timestamp(time1, timeType) - str2timestamp(time2, timeType)

@verison_warning
def sub_datetime(beginDate: datetime.datetime, endDate: datetime.datetime) -> (int, int):
    date_byyear = datetime.datetime(year=endDate.year, month=beginDate.month, day=beginDate.day)
    if beginDate.year == endDate.year:
        year = 0
        days = (endDate - date_byyear).days
        if endDate.day >= beginDate.day:
            months = endDate.month - beginDate.month
        else:
            months = endDate.month - beginDate.month - 1
    elif beginDate.year < endDate.year:
        if date_byyear.__le__(endDate):  # 纪念日到了
            year = endDate.year - beginDate.year
            days = (endDate - date_byyear).days
            if endDate.day < beginDate.day:
                months = endDate.month - beginDate.month - 1
            else:
                months = endDate.month - beginDate.month
        else:  # 纪念日没到
            year = endDate.year - beginDate.year - 1
            days = 365 + (endDate - date_byyear).days
            if endDate.day >= beginDate.day:
                months = 12 + endDate.month - beginDate.month
            else:
                months = endDate.month - beginDate.month + 11
    else:
        year, months, days = 0, 0, 0
    return year, months, days

cpdef (int, int) countTime_NewYear(beginDate: datetime.datetime, endDate: datetime.datetime):
    '''
    每年元旦记为1年,每月1号记为1月
    :param beginDate:
    :param endDate:
    :return:
    '''
    cdef int years, month
    if beginDate.year != endDate.year:
        if beginDate.month == beginDate.day == 1:
            years = endDate.year - beginDate.year + 1
            month = 0
        else:
            years = endDate.year - beginDate.year
            if beginDate.day == 1:
                month = 12 - beginDate.month + 1
            else:
                month = 12 - beginDate.month
    else:
        years = 0
        if beginDate.day != 1:
            month = endDate.month - beginDate.month
        else:
            month = endDate.month - beginDate.month + 1
    return years, month

cpdef int sub_datetime_Bydayone(beginDate: datetime.datetime, endDate: datetime.datetime):
    '''
    计算两个时间间隔多少个月,每月1号计
    :param beginDate:
    :param endDate:
    :return:
    '''
    cdef int totalmonth, years, beginMonth
    if beginDate.year != endDate.year:
        years = endDate.year - beginDate.year - 1
        if beginDate.day != 1:
            beginMonth = 12 - beginDate.month
        else:
            beginMonth = 12 - beginDate.month + 1
        totalmonth = years * 12 + beginMonth + endDate.month
    else:
        if beginDate.day != 1:
            totalmonth = endDate.month - beginDate.month
        else:
            totalmonth = endDate.month - beginDate.month + 1
    return totalmonth
    # if beginDate.day > 1:
    #     months = endDate.month - beginDate.month
    # else:
    #     months = endDate.month - beginDate.month + 1
    # if beginDate.month == beginDate.day == 1:
    #     year = endDate.year - beginDate.year + 1
    # else:
    #     year = endDate.year - beginDate.year
    # return year, months

cpdef bint isVaildDate(str date, str timeType="%Y-%m-%d %H:%M:%S"):
    '''
    判断字符串是否为某种时间格式
    :param date:
    :param timeType:
    :return:
    '''
    try:
        time.strptime(date, timeType)
        return True
    except:
        return False

cpdef dict getChlidType(dbcon: pymssql.Connection):
    '''
    返回所有积分类型的所有子类型
    :param dbcon: MSSQL连接器
    :return:
    '''
    rewardPointsType_df = read_sql(
        'select RewardPointsTypeID,ParentID,ChildrenID,RewardPointsTypeCode,Name from RewardPointsType where DataStatus=0',
        dbcon)
    cdef list childID, childID_list, childName
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
                    rewardPointsType_df['RewardPointsTypeID'].isin(childID_list), 'Name'].tolist()
                childID = rewardPointsType_df.loc[
                    rewardPointsType_df['RewardPointsTypeID'].isin(childID_list), 'ChildrenID'].tolist()
                _rewardPointsType_container.extend(childName)
            else:
                break
        res[_name] = _rewardPointsType_container
    return res

cpdef str get_dfUrl(df: DataFrame, str Operator):
    cdef str filename = f"{Operator}{str(time.time())}.xlsx"
    cdef str filepath = DOWNLOAD_FOLDER + '/' + filename
    df.to_excel(filepath, index=False, encoding='utf-8')
    return "/download/" + filename  # 传回相对路径

def isEmpty(obj):
    '''
    判断是否为空，空:Ture
    :param obj:
    :return:
    '''
    if isinstance(obj, int):
        return False
    elif obj is None:
        return True
    elif isinstance(obj, dict):
        return obj == {}
    elif isinstance(obj, str):
        return obj == ''
    elif isinstance(obj, list):
        return obj == []
    elif isinstance(obj, tuple):
        return obj == ()
    elif isinstance(obj, DataFrame):
        return len(obj.index) == 0
    else:
        return isna(obj)


@verison_warning
class MyEncoder(json.JSONEncoder):
    def default(self, obj: object):
        if isinstance(obj, float) and isnan(obj):
            return 0
        elif isinstance(obj, integer):
            return int(obj)
        elif isinstance(obj, floating):
            return float(obj)
        elif isinstance(obj, ndarray):
            return obj.tolist()
        elif isinstance(obj, Timestamp):
            return datetime_string(to_datetime(obj, "%Y-%m-%d %H:%M:%S"))
        elif isinstance(obj, bytes):
            return str(obj, 'utf-8')
        else:
            return super().default(obj)


class SuperEncoder(simplejson.JSONEncoder):
    def default(self, obj: object):
        if isinstance(obj, integer):
            return int(obj)
        elif isinstance(obj, floating):
            return float(obj)
        elif isinstance(obj, ndarray):
            return obj.tolist()
        elif isinstance(obj, Timestamp):
            return datetime_string(to_datetime(obj, "%Y-%m-%d %H:%M:%S"))
        elif isinstance(obj, bytes):
            return str(obj, 'utf-8')
        else:
            return super().default(obj)
