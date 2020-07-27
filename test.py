# utf-8
from config.dbconfig import mssqldb, ncdb
from Interface import RewardPointInterface
import pandas as pd
worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
# data = {"jobid": 100297, }
data = {"Operator": 100055}
ff=pd.read_excel(r"C:\Users\100236\Desktop\商品导入模板.xlsx")
res= worker.import_goods(data_in=data,file_df=ff)
print(res)



