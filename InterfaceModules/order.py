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
    def createOrder(self, data_in: dict):
        cur = self.db_mssql.cursor()
        Operator = data_in.get("Operator")


        # 获取购物车信息
        shoppingCartList = self.getShoppingCartList(JobId=Operator)
        print(shoppingCartList)
        totalNum = 0
        totalPrice = 0
        isOverStock = False
        errMsg=''
        # 判断购物车商品是否超过库存,若超过库存，给出提示：某某商品超过库存，请修改购物车数量
        for item in shoppingCartList:
            if isOverStock:
                print(isOverStock)
                return
            else:
                name = item.get("Name")
                stock = item.get("TotalIn") - item.get("TotalOut")
                num = item.get("GoodsAmount")
                price = item.get("PointCost")
                goodsTotalPrice = num * price
                totalNum = num + totalNum
                totalPrice = goodsTotalPrice + totalPrice
                if num>stock:
                    isOverStock = True
                    errMsg = name + '超过库存，请修改购物车数量'

        if isOverStock:
            return False, errMsg
        # 在订单表生成一条记录
        insert_sql = "insert into [RewardPointDB].[dbo].[PointOrder] ({}) values ({})"
        sql_item = ["OrderStatus", "DataStatus", "JobId", "CreatedBy", "TotalPrice", "TotalNum"]
        sql_values = ["1", "0", Operator, Operator, totalPrice, totalNum]
        for item in sql_values:
            if isinstance(item, str):  # 如果是字符串 加上引号
                item = "\'" + item + "\'"
        sql = insert_sql.format(','.join(sql_item), ','.join(list(map(str, sql_values))))
        cur.execute(sql)
        # 生成之后获取该记录ID
        PointOrderID = cur.lastrowid
        # 将商品的信息和订单号存入订单商品表
        insert_order_goods_base_sql = "insert into [RewardPointDB].[dbo].[PointOrderGoods] ({}) values ({})"
        sql_order_goods_item = ["PointOrderID", "GoodsID", "OrderGoodsAmount", "CreatedBy", "DataStatus"]

        insert_stock_out_base_sql = "insert into [RewardPointDB].[dbo].[StockOutDetail] ({}) values ({})"
        sql_tock_out_item = ["GoodsID", "PointOrderGoodsID", "ChangeType", "ChangeAmount", "DataStatus", "CreatedBy", "PointOrderID"]

        try:
            for spItem in shoppingCartList:
                print("循环开始")

                for item in sql_values:
                    if isinstance(item, str):  # 如果是字符串 加上引号
                        item = "\'" + item + "\'"
                sql_order_goods_values = [PointOrderID, spItem.get("GoodsID"), spItem.get("GoodsAmount"), Operator, 0]
                insert_order_goods_sql = insert_order_goods_base_sql.format(','.join(sql_order_goods_item), ','.join(list(map(str, sql_order_goods_values))))
                print(insert_order_goods_sql)
                cur.execute(insert_order_goods_sql)
                # 商品出库表新增数据，状态均为锁定
                sql_stock_out_values = [spItem.get("GoodsID"), cur.lastrowid, 1, spItem.get("GoodsAmount"), 0, Operator, PointOrderID]
                insert_stock_out_sql = insert_stock_out_base_sql.format(','.join(sql_tock_out_item), ','.join(list(map(str, sql_stock_out_values))))
                print(insert_stock_out_sql)
                cur.execute(insert_stock_out_sql)
                # 将购物车的记录状态变成无效
                update_shopping_cart_status_sql = "update ShoppingCart set DataStatus=1 where ShoppingCartID = %d" % (spItem.get("ShoppingCartID"))
                print(update_shopping_cart_status_sql)
                cur.execute(update_shopping_cart_status_sql)
            self.db_mssql.commit()
            return True, ''
        except Exception as e:
            self.db_mssql.rollback()
            print(e)
            return False, e

    #查询订单列表
    def getOrderList(self, data_in: dict):
        pass
    # 查看订单详情
    # 确定订单
    def confirmOrder(self, data_in:dict):
        print(data_in)
        base_sql = "update PointOrder set OrderStatus=2 where PointOrderID = %d" % (data_in.get("PointOrderID"))
        print(base_sql)
        cur = self.db_mssql.cursor()
        cur.execute(base_sql)
        self.db_mssql.commit()
        return True
    # 退回订单
    def rejectOrder(self, data_in:dict):
        cur = self.db_mssql.cursor()
        try:
            # 将订单状态变成已退回
            base_sql = "update PointOrder set OrderStatus=3 where PointOrderID = %d" % (data_in.get("PointOrderID"))
            cur.execute(base_sql)
            # 将出库单中的数据变成无效
            update_sql = "UPDATE StockOutDetail SET DataStatus = 1 WHERE PointOrderID = %d" % (data_in.get("PointOrderID"))
            cur.execute(update_sql)
            self.db_mssql.commit()
            return True
        except Exception as e:
            self.db_mssql.rollback()
            print(e)
            return False


    # 确认收货
    def finishOrder(self, data_in:dict):
        cur = self.db_mssql.cursor()
        try:
            # 改变订单状态
            update_order_status_sql = "update PointOrder set OrderStatus=0 where PointOrderID = %d" % (data_in.get("PointOrderID"))
            cur.execute(update_order_status_sql)
            #订单商品出库
            update_tock_info_sql = "update StockOutDetail set ChangeType=0, StockOutOperator=\'%s\', StockOutTime=\'%s\' where PointOrderID = %d" % (data_in.get("Operator"), data_in.get("time"), data_in.get("PointOrderID"))
            print(update_tock_info_sql)
            cur.execute(update_tock_info_sql)
            self.db_mssql.commit()
            return True
        except Exception as e:
            self.db_mssql.rollback()
            print(e)
            return False
    # 根据工号获取购物车列表
    def getShoppingCartList(self, JobId):
        print("getCartList")
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
                           AND sc.JobId = %s''' % (JobId)
        res = pd.read_sql(sql=sql, con=self.db_mssql)
        return df_tolist(res)
