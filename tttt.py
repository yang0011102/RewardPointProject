# utf-8
import simplejson
import requests
import pandas as pd
from tool.tool import *
head={"Accept":"application/json",
      "Content-Type":"application/json",
      "Origin":"null",
      }
data={'page': 1, 'pageSize': 10,'Status':0}
_response = requests.get(url="http://127.0.0.1:5000/get_token/jobid:1111111",
                          # data=simplejson.dumps(data),headers=head
                          )
print(_response.json())