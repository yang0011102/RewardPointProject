# utf-8
import simplejson
import requests
import pandas as pd
from tool.tool import *

head = {"Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "null",
        }
# data = {"jobid": 100297,"page": 1, "pageSize": 10,"rewardPointsType": "管理积分"}
# _response = requests.post(url="http://222.186.81.37:5000/Interface/query_rewardPoint",
#                           data=simplejson.dumps(data), headers=head
#                           )
_response = requests.post(url="http://222.186.81.37:5000/Interface/query_FixedPoints_ByYear",
                          data=simplejson.dumps({'jobid': 100297, 'year': 2020}), headers=head
                          )

print(_response.json())