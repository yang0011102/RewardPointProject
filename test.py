# utf-8

from config.dbconfig import mssqldb as mssqlDbInfo
from datetime import datetime
from tool.tool import *
import pymssql
db_mssql = pymssql.connect(**mssqlDbInfo)

getChlidType(db_mssql)