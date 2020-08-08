# utf-8

from config.dbconfig import mssqldb as mssqlDbInfo
from datetime import datetime
from tool import sub_datetime_Bydayone

print(sub_datetime_Bydayone(datetime(year=2015,month=1,day=3),datetime(year=2019,month=3,day=7)))
