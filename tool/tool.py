# utf-8

import json
import time

import numpy as np
import pandas as pd

from config.config import DOWNLOAD_FOLDER


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return datetime_string(pd.to_datetime(obj, "%Y-%m-%d %H:%M:%S"))

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

def getChlidType(dbcon) -> dict:
    '''
    返回所有积分类型的所有子类型
    :param dbcon: MSSQL连接器
    :return:
    '''
    rewardPointsType_df = pd.read_sql(
        'select RewardPointsTypeID,ParentID,ChildrenID,RewardPointsTypeCode,Name from RewardPointsType where DataStatus=0',
        dbcon)
    res = {}
    for _name in rewardPointsType_df['Name'].tolist():
        _rewardPointsType_container = []
        # 遍历多叉
        childID = rewardPointsType_df.loc[
            rewardPointsType_df['Name'] == _name, 'ChildrenID'].tolist()
        while True:
            while None in childID:  # 删除空
                childID.remove(None)
            if len(childID) != 0:
                childID_list = childID[0].split(',')
                childName = rewardPointsType_df.loc[
                    rewardPointsType_df['RewardPointsTypeID'].isin(childID_list), 'Name'].values
                childID = rewardPointsType_df.loc[
                    rewardPointsType_df['RewardPointsTypeID'].isin(childID_list), 'ChildrenID'].tolist()
                _rewardPointsType_container.extend(childName)
            else:
                break
        res[_name] = _rewardPointsType_container
    return res

def get_dfUrl(df: pd.DataFrame, Operator: str) -> str:
    filename = Operator + str(time.time()) + ".xlsx"
    print('文件名', filename)
    filepath = DOWNLOAD_FOLDER + '/' + filename
    print('文件路径', filepath)
    df.to_excel(filepath, index=False)
    print('保存到', filepath)
    return "http://192.168.40.229:8080/download/" + filename  # 传回相对路径


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
