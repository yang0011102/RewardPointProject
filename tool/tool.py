# utf-8

import json
import time

import numpy
import pandas as pd


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


def df_tolist(df: pd.DataFrame):
    res_list = []
    for _index in df.index.tolist():
        res_list.append(df.loc[_index, :].to_dict())
    return res_list


def datetime_string(time, timeType="%Y-%m-%d %H:%M:%S"):
    return time.strftime(timeType)


def isVaildDate(date, timeType="%Y-%m-%d %H:%M:%S"):
    '''
    判断字符串是否为某种时间格式
    :param date:
    :param timeType:
    :return:
    '''
    try:
        time.strptime(date, timeType)
        return True
    except:
        return False


def isEmpty(obj):
    '''
    判断是否为空，空:Ture
    :param obj:
    :return:
    '''
    if isinstance(obj, int):
        return False
    elif obj is None:
        return True
    elif isinstance(obj, dict):
        return obj == {}
    elif isinstance(obj, str):
        return obj == ''
    elif isinstance(obj, list):
        return obj == []
    elif isinstance(obj, tuple):
        return obj == ()
    elif isinstance(obj, pd.DataFrame):
        return len(obj.index) == 0
    else:
        return pd.isna(obj)


if __name__ == "__main__":
    fff = pd.DataFrame([[1, 2, 3],
                        [5, 'hhh', '1'],
                        ["收", "s", 1]], columns=["第一", "第二", "第三"])
    print(fff.to_json(orient='records'))
    print(json.dumps(df_tolist(fff)))
