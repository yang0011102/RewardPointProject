# utf-8
mssqldb = {"host": "192.168.40.229:1433", "user": "serverapp", "password": "wetown2020", "database": "RewardPointDB",
           "charset": "utf8"}
import pymssql

mssql_con = pymssql.connect(host="192.168.40.229:1433", user="serverapp", password="wetown2020",
                            database="RewardPointDB",
                            charset="utf8")
sql_word = "select Reason,ReasonType from RewardPointDB.dbo.RewardPointsDetail where RewardPointsdetailID=28194"
cursor = mssql_con.cursor()
cursor.execute(sql_word)
print(cursor.fetchall()[0])
# import pandas as pd
#
# df = pd.read_sql("select Reason from RewardPointDB.dbo.RewardPointsDetail where RewardPointsdetailID=28194",
#                  con=mssql_con)
# print(df)
