# utf-8
import simplejson
import requests
import pandas as pd
from tool.tool import *
head={"Accept":"application/json",
      "Content-Type":"application/json",
      "Origin":"null",
      }
data={'jobid':100297}
_response = requests.post(url="http://192.168.40.161:8080/Interface/query_rewardPoint",
                          data=simplejson.dumps(data),headers=head
                          )
print(_response.json())