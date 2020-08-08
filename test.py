# utf-8

from config.dbconfig import mssqldb as mssqlDbInfo
import pymssql
from tool import getChlidType
db_mssql = pymssql.connect(**mssqlDbInfo)
print(getChlidType(db_mssql))
