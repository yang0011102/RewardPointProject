# utf-8
import json
import requests
import pandas as pd
head={"Accept":"application/json",
      "Content-Type":"application/json",
      "Origin":"null",
      }
data_in = {'PointOrderID': '29'}
_response = requests.post(url="http://192.168.40.161:8080/Interface/query_orderDetail",
                          data=json.dumps(data_in),headers=head
                          )
print(_response.json())
