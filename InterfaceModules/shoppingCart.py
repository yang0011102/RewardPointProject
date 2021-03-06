# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql

from tool import *

import os
basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ShoppingCartInterface:
    def __init__(self, mssqlDbInfo: dict, ncDbInfo: dict):
        # 数据库连接体
        self.db_mssql = pymssql.connect(**mssqlDbInfo)
        self.db_nc = cx_Oracle.connect(
            f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
            encoding="UTF-8", nencoding="UTF-8")

    # 添加商品进入购物车
    def addCart(self, data_in:dict):
        # 判断数量是否超过库存
        isOverStock = self.isOverStock(data_in=data_in)

        # 若不超过，判断商品是否已在购物车
        if isOverStock:
            return {
                "code": -1,
                "msg": '库存不足'
            }
        else:
            flag, ShoppingCartID, GoodsAmount = self.hasExist(data_in=data_in)

            # 若在，修改数量
            if flag:
                num = data_in.get("num")
                editNum = num + GoodsAmount
                resFlag = self.changeCartNum(editNum=editNum, ShoppingCartID=ShoppingCartID)

                if resFlag:
                    return {
                        "code": 0,
                        "msg": ""
                    }
                else:
                    return {
                        "code": -1,
                        "msg": "修改购物车数量失败"
                    }
            else:
                # 若不在，新增
                base_sql = "insert into [RewardPointDB].[dbo].[ShoppingCart] ({}) values ({})"
                cur = self.db_mssql.cursor()
                Operator = data_in.get("Operator")
                num = data_in.get("num")
                GoodsID = data_in.get("GoodsID")
                sql_item = ["Status", "DataStatus", "GoodsAmount", "JobId", "CreatedBy", "GoodsID"]
                sql_values = ["1", "0", num, Operator, Operator, GoodsID]
                for item in sql_values:
                    if isinstance(item, str):  # 如果是字符串 加上引号
                        item = "\'" + item + "\'"
                sql = base_sql.format(','.join(sql_item), ','.join(list(map(str, sql_values))))
                cur.execute(sql)

                self.db_mssql.commit()
                return {
                    "code": 0,
                    "msg": ""
                }

    # 判断数量是否超过库存
    def isOverStock(self, data_in:dict):
        # 定义标识，flag为True:超过，False:不超过
        flag = False
        num = data_in.get("num")

        # 根据商品ID查询库存
        stock = self.getStockById(data_in=data_in)

        # 将数量和库存相比，小于返回False,大于返回Ture
        if num > stock:
            flag = True
        else:
            flag = False

        return flag

    # 修改购物车的数量
    def changeCartNum(self, editNum, ShoppingCartID):
        shopping_cart_id = ShoppingCartID
        num = editNum
        base_sql = "update ShoppingCart set GoodsAmount=%d where shoppingCartID = %d" % (num, shopping_cart_id)
        cur = self.db_mssql.cursor()
        cur.execute(base_sql)
        self.db_mssql.commit()
        return True

    # 删除购物车商品
    def deleteCart(self, data_in:dict):
        base_sql = "update ShoppingCart set Status=2, DataStatus=1 where shoppingCartID = %s" % (data_in.get("ShoppingCartID"))
        cur = self.db_mssql.cursor()
        cur.execute(base_sql)
        self.db_mssql.commit()
        return True

    # 查询购物车列表
    def getCartList(self, data_in:dict):
        JobId = data_in.get("Operator")
        sql = '''SELECT sc.ShoppingCartID,sc.GoodsID,sc.GoodsAmount,g.PictureUrl,g.Name,g.PointCost,g.ChargeUnit, ISNULL(ti.TotalIn, 0) AS TotalIn , ISNULL(tout.TotalOut, 0) AS TotalOut
                    FROM ShoppingCart sc
                    INNER JOIN Goods g ON sc.GoodsID = g.GoodsID
                    LEFT JOIN (
                                            SELECT sid.GoodsID, SUM(sid.ChangeAmount) as TotalIn
                                            FROM StockInDetail sid
                                            WHERE sid.DataStatus = 0
                                            AND sid.ChangeType = 0
                                            GROUP BY sid.GoodsID
                    ) AS ti ON ti.GoodsID = sc.GoodsID
                    LEFT JOIN (
                                            SELECT sod.GoodsID, SUM(sod.ChangeAmount) as TotalOut
                                            FROM StockOutDetail sod
                                            WHERE sod.DataStatus = 0
                                            GROUP BY sod.GoodsID
                    ) AS tout ON tout.GoodsID = sc.GoodsID
                    WHERE sc.DataStatus = 0
                    AND sc.Status = 1
                    AND g.DataStatus = 0
                    AND g.Status = 0
                    AND sc.JobId = %s'''%(JobId)
        res = read_sql(sql=sql, con=self.db_mssql)
        return res

    # 判断商品是否已在购物车
    def hasExist(self, data_in: dict):
        GoodsID = data_in.get("GoodsID")
        Operator = data_in.get("Operator")
        sql = "SELECT * FROM ShoppingCart WHERE Status = 1 AND DataStatus = 0 AND JobId=%s AND GoodsID = %d" %(Operator,GoodsID)
        res = read_sql(sql=sql, con=self.db_mssql)
        if len(df_tolist(res)) > 0:
            flag = True
            ShoppingCartID = df_tolist(res)[0].get("ShoppingCartID")
            GoodsAmount = df_tolist(res)[0].get("GoodsAmount")
        else:
            flag = False
            ShoppingCartID = '',
            GoodsAmount = ''
        return flag, ShoppingCartID, GoodsAmount

    # 查询商品库存
    def getStockById(self, data_in:dict):
        id = data_in.get("GoodsID")
        TotalIn = self.getGoodsTotalIn(id)
        TotalOut = self.getGoodsTotalOut(id)
        stock = TotalIn - TotalOut
        return stock

    # 查询商品的总入库
    def getGoodsTotalIn(self, GoodsID):
        sql = "SELECT sid.GoodsID, SUM(sid.ChangeAmount) AS TotalIn FROM StockInDetail sid WHERE sid.DataStatus = 0 AND sid.GoodsID = %s GROUP BY sid.GoodsID"%(GoodsID)
        info = read_sql(sql=sql, con=self.db_mssql)
        TotalIn = 0
        if len(df_tolist(info)) > 0:
            detail = df_tolist(info)[0]
            TotalIn = detail.get("TotalIn")

        return TotalIn

    #查看商品的总出库
    def getGoodsTotalOut(self, GoodsID):
        sql = "SELECT sod.GoodsID, SUM(sod.ChangeAmount) AS TotalOut FROM StockOutDetail sod WHERE sod.DataStatus = 0 AND sod.GoodsID = %s GROUP BY sod.GoodsID" % (
            GoodsID)
        info = read_sql(sql=sql, con=self.db_mssql)
        TotalOut = 0
        if len(df_tolist(info)) > 0:
            detail = df_tolist(info)[0]
            TotalOut = detail.get("TotalOut")

        return TotalOut