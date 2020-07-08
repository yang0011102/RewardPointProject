# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql

from config.config import DOWNLOAD_FOLDER
from tool.tool import *

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
        print("上传开始")

        # 定义一个图片存放的位置 存放在static下面
        path = basedir + "/static/uploadImgs/"
        print(path)

        # 图片名称
        imgName = img.filename
        tempList = imgName.split('.')
        length = len(tempList)
        print(tempList)

        imgType = tempList[length-1]
        print(imgType)

        filename = str(data_in.get("Operator")) + str(time.time()) +"."+imgType
        # 图片path和名称组成图片的保存路径
        file_path = path + filename
        print(file_path)

        # 保存图片
        img.save(file_path)

        print('保存成功')

        # url是图片的路径
        url = '/static/uploadImgs/' + filename
        return url