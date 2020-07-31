# utf-8
import simplejson
import requests
import pandas as pd
from tool.tool import *
head={"Accept":"application/json",
      "Content-Type":"application/json",
      "Origin":"null",
      }
data={'page': 1, 'pageSize': 10,'jobid':100297}
_response = requests.post(url="http://222.186.81.37:5000/Interface/query_RewardPointSummary",
                          data=simplejson.dumps(data),headers=head
                          )
print(_response.json())