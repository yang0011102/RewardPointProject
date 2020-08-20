# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql
from time import time
import os
basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class UploadInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")

    def editorData(self, data_in: dict, img):

        # 定义一个图片存放的位置 存放在static下面
        path = basedir + "/static/uploadImgs/"

        # 图片名称
        imgName = img.filename
        tempList = imgName.split('.')
        length = len(tempList)
        imgType = tempList[length-1]
        filename = str(data_in.get("Operator")) + str(time()) +"."+imgType
        # 图片path和名称组成图片的保存路径
        file_path = path + filename

        # 保存图片
        img.save(file_path)

        # url是图片的路径
        # url = '/static/uploadImgs/' + filename
        url = '/uploadImgs/' + filename
        return url
