# utf-8
# from Interface import RewardPointInterface
# from config.dbconfig import *
#
# data={"jobid":"100283"}
# worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
# res=worker.query_FixedPointDetail(data_in=data)
# print(res)

import datetime


def countTime_NewYear(beginDate: datetime.datetime, endDate: datetime.datetime):
    '''
    每年元旦记为1年,每月1号记为1月
    :param beginDate:
    :param endDate:
    :return:
    '''
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


beginDate = datetime.datetime(year=2012, month=1, day=2)
endDate = datetime.datetime(year=2020, month=12, day=31)
print(countTime_NewYear(beginDate, endDate))
