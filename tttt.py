# utf-8
import json
import requests

head={"Accept":"application/json",
      "Content-Type":"application/json",
      "Origin":"null",
      }
data_in = {
    "page": 1, "pageSize": 10}
_response = requests.post(url="http://192.168.40.161:8080/Interface/query_activity",
                          data=json.dumps(data_in, ),headers=head
                          )


print(_response.json())
