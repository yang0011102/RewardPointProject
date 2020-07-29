# utf-8
import simplejson
import requests
import pandas as pd
from tool.tool import *
head={"Accept":"application/json",
      "Content-Type":"application/json",
      "Origin":"null",
      }
data={'page': 1, 'pageSize': 10}
_response = requests.post(url="http://192.168.40.161:8080/Interface/query_FixedPoints",
                          data=simplejson.dumps(data),headers=head
                          )
print(_response.json())