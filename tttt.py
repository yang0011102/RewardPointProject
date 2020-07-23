# utf-8
import json

import requests


data_in = {
    "page": 1, "pageSize": 10}
_response = requests.post(url="http://192.168.40.161:8080/Interface/query_RewardPointSummary",
                          data=json.dumps(data_in, )
                          )


print(_response.json())
