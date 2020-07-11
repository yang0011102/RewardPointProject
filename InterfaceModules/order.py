# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql

from tool.tool import *

import os
basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class OrderInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")

    # 创建订单
    def createOrder(self, data_in:dict):
        GoodsIDs = data_in.get("GoodsIDs")
        ShoppingCartIDs = data_in.get("ShoppingCartIDs")
        cur = self.db_mssql.cursor()
        Operator = data_in.get("Operator")
        # 在订单表生成一条记录
        insert_sql = "insert into [RewardPointDB].[dbo].[PointOrder] ({}) values ({})"
        sql_item = ["OrderStatus", "DataStatus", "JobId", "CreatedBy"]
        sql_values = ["1", "1", Operator, Operator]
        for item in sql_values:
            if isinstance(item, str):  # 如果是字符串 加上引号
                item = "\'" + item + "\'"
        sql = insert_sql.format(','.join(sql_item), ','.join(list(map(str, sql_values))))
        print(sql)
        cur.execute(sql)
        print(cur.lastrowid)
        # 生成之后获取该记录ID
        PointOrderID = cur.lastrowid
        # self.db_mssql.commit()
        # 根据GoodsID获取商品的信息
        print(PointOrderID)
        print(GoodsIDs)
        # 将商品的信息和订单号存入订单商品表
        # 商品出库表新增数据，状态均为锁定
        # 将购物车的记录状态变成无效
        # 将商品的TotalOut更新
        return True
    # 查看订单
    # 查看订单详情
    # 确定订单
    # 退回订单
    # 确认收货