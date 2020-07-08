# utf-8
import json

import requests

base_sql = "select top pageSize RewardPointsdetailID,DepartmentLv1,DepartmentLv2,DepartmentLv3," \
           "FunctionalDepartment,Submit,Post,RewardPointsType,{},ChangeType,ChangeAmount,Reason,Proof," \
           "ReasonType,JobId,AssessmentDate,IsAccounted " \
           "from (select row_number() over(order by RewardPointsdetailID asc) as rownumber ,* " \
           "from [RewardPointDB].[dbo].[RewardPointsDetail]) temp_row  where "
data_in = {
    # 'name': '陈明姣', 'jobid': 100297,
    "Operator": 100297}
_response = requests.post(url="http://192.168.40.229:8080/Interface/export_rewardPoint",
                          data=json.dumps(data_in, )
                          )


print(_response)
