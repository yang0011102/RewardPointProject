# coding:utf-8
'''
接口主体
'''

import cx_Oracle
import pymssql

from tool.tool import *

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
        print(isOverStock)

        # 若不超过，判断商品是否已在购物车
        if isOverStock:
            return {
                "code": -1,
                "msg": '库存不足'
            }
        else:
            flag, ShoppingCartID, GoodsAmount = self.hasExist(data_in=data_in)
            print(flag)
            print(ShoppingCartID)

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
                sql_values = ["1", "1", num, Operator, Operator, GoodsID]
                for item in sql_values:
                    if isinstance(item, str):  # 如果是字符串 加上引号
                        item = "\'" + item + "\'"
                sql = base_sql.format(','.join(sql_item), ','.join(list(map(str, sql_values))))
                print(sql)
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
        print(base_sql)
        cur = self.db_mssql.cursor()
        cur.execute(base_sql)
        self.db_mssql.commit()
        return True

    # 删除购物车商品
    def deleteCart(self, data_in:dict):
        base_sql = "update ShoppingCart set Status=2, DataStatus=2 where shoppingCartID = %d" % (data_in.get("shoppingCartID"))
        print(base_sql)
        cur = self.db_mssql.cursor()
        cur.execute(base_sql)
        self.db_mssql.commit()
        return True

    # 查询购物车列表
    def getCartList(self, data_in:dict):
        JobId = data_in.get("Operator")
        sql = "SELECT sc.ShoppingCartID,sc.GoodsID,sc.GoodsAmount,g.PictureUrl,g.Name,g.Price,g.ChargeUnit,g.TotalIn-g.TotalOut as stock FROM ShoppingCart sc LEFT JOIN Goods g ON g.GoodsID = sc.GoodsID WHERE sc.JobId = %s AND sc.Status = 1 AND sc.DataStatus = 1"%(JobId)
        res = pd.read_sql(sql=sql, con=self.db_mssql)

        return res
    # 判断商品是否已在购物车
    def hasExist(self, data_in: dict):
        GoodsID = data_in.get("GoodsID")
        Operator = data_in.get("Operator")
        sql = "SELECT * FROM ShoppingCart WHERE Status = 1 AND DataStatus = 1 AND JobId=%s AND GoodsID = %d" %(Operator,GoodsID)
        res = pd.read_sql(sql=sql, con=self.db_mssql)
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
        sql = "SELECT TotalIn,TotalOut FROM Goods WHERE GoodsID = %d" % (id)
        info = pd.read_sql(sql=sql, con=self.db_mssql)
        detail = df_tolist(info)[0]
        TotalIn = detail.get("TotalIn")
        TotalOut = detail.get("TotalOut")
        stock = TotalIn - TotalOut
        return stock