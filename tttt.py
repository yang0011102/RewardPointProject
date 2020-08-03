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
_response = requests.post(url="http://222.186.81.37:5000/Interface/getUserInfo",
                          data=simplejson.dumps({'code': "a4734373ca473f13b8947a4ac380fbf0",
                                                 'corpId': "dingcd0f5a2514db343b35c2f4657eb6378f"}), headers=head
                          )

print(_response.json())
