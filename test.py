# utf-8
# from config.dbconfig import mssqldb, ncdb
# from Interface import RewardPointInterface
# import pandas as pd

index_list=[0,1,2,3,4,5,6,7,8,9]
ss=1
ct=[]
c=0
while True:
    if c > len(index_list)-1:
        break
    ct.append(index_list[c: c + ss])
    c+=ss
print(ct)