# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql

from tool.tool import *

from dingtalk.client import SecretClient
import requests

import os
basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class DDInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")
        self.url = "https://oapi.dingtalk.com"

    def getUserInfo(self, data_in:dict):
        key = "dingawmt9uvjq3g93ilz"
        secret = "59AAuG9MvJFvU6dLlC3yOTeTTRtS4zcZIpqfQJl_Q6lAUy3Lnmc8Sm1_Mht6ZUY2"
        resp = requests.get(
            url=self.url + "/gettoken",
            params=dict(appkey=key, appsecret=secret)
        )
        resp = resp.json()
        access_token = resp["access_token"]
        code = data_in.get("code")
        user_info = self.get_user_info(code=code, token=access_token)
        userid = user_info.get("userid")
        user = self.get_user(access_token=access_token, userid=userid)
        return user

    def get_user_info(self, code, token):
        resp = requests.get(
            url=self.url+"/user/getuserinfo",
            params=dict(access_token=token, code=code)
        )
        resp = resp.json()
        return resp

    def get_user(self, access_token, userid):
        resp = requests.get(
            url=self.url+"/user/get",
            params=dict(access_token=access_token, userid=userid)
        )
        resp = resp.json()
        return resp