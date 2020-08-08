# utf-8
'''
改写lf else版本的dispather
使用字典装载方法索引
'''
import os
from Interface import RewardPointInterface
from InterfaceModules.activity import ActivityInterface
from InterfaceModules.upload import UploadInterface
from InterfaceModules.shoppingCart import ShoppingCartInterface
from InterfaceModules.order import OrderInterface
from config.dbconfig import *
from pre_check import *
from tool.tool import *

worker = RewardPointInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
uploadWorker = UploadInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
activityWorker = ActivityInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
shoppingCartWorker = ShoppingCartInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)
orderWorker = OrderInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)

def dispatcher(str selector,dict data, files=None):
    '''
    根据selector调度接口
    :param selector:
    :param data:
    :param files:
    :return:
    '''

    def query_rewardPoint() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_query_rewardPointDetail,
                                                                    'check_exist': check_query_rewardPointDetail,
                                                                    'check_dateType': check_dateType_rewardPointDetail,
                                                                    })

        if flag:
            totalLength, res_df = worker.query_rewardPoint(data_in=data)
            _response = {"code": 0,
                         "msg": "",
                         "data": {"totalLength": totalLength,
                                  "detail": df_tolist(res_df), }
                         }
        return _response

    def import_rewardPoint() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_import_rewardPoint,
                                                                    'check_exist': check_import_rewardPoint,
                                                                    },
                                    mustFile={'check_filetype': filetype_rewardPoint,
                                              'table_column': import_rewardPoint_columncheck,
                                              'table_dateType': file_dateType_rewardPoint})
        if not flag:
            return _response
        if worker.import_rewardPoint_onesql(data_in=data,
                                            file_df=pd.read_excel(files.stream), ):
            _response = {"code": 0,
                         "msg": "",
                         }
        else:
            _response = {"code": 103,
                         "msg": "未知原因",
                         }
        return _response

    def delete_rewardPoint() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_delete_rewardPoint,
                                                                    'check_exist': check_delete_rewardPoint})
        if flag:
            if worker.delete_rewardPoint(data_in=data):
                _response = {"code": 0,
                             "msg": "", }
            else:
                _response = {"code": 104,
                             "msg": "未能执行删除", }
        return _response

    def export_rewardPoint() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_export_rewardPoint,
                                                                    'check_exist': check_export_rewardPoint},
                                    )
        if flag:
            _response = {"code": 0,
                         "msg": "",
                         "data": worker.export_rewardPoint(data_in=data),
                         }
        return _response

    def account_rewardPoint() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_account_rewardPoint,
                                                                    'check_exist': check_account_rewardPoint},
                                    )
        if flag:
            flag = worker.account_rewardPoint(data_in=data)
            if flag:
                _response = {"code": 106, "msg": "缺少参数"}
            else:
                _response = {"code": 0, "msg": ""}
        return _response

    def query_RewardPointSummary() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_query_RewardPointSummary,
                                                                    'check_exist': check_query_RewardPointSummary,
                                                                    })
        if flag:
            totalLength, res_df = worker.query_RewardPointSummary(data_in=data)
            _response = {"code": 0,
                         "msg": "",
                         "data": {"totalLength": totalLength,
                                  "detail": df_tolist(res_df), }
                         }
        return _response

    def export_RewardPointSummary() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_export_RewardPointSummary,
                                                                    'check_exist': check_export_RewardPointSummary},
                                    )
        if flag:
            _response = {"code": 0,
                         "msg": "",
                         "data": worker.export_RewardPointSummary(data_in=data),
                         }
        return _response

    def query_B_RewardPoint() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_B_RewardPoint,
                                                                    'check_exist': check_B_RewardPoint,
                                                                    })
        if flag:
            _response = {"code": 0,
                         "msg": "",
                         "data": worker.query_FixedPointDetail(data_in=data)
                         }
        return _response

    def import_goods() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_import_goods,
                                                                    'check_exist': check_import_goods,
                                                                    },
                                    mustFile={'check_filetype': filetype_import_goods,
                                              'table_column': import_goods_columncheck,
                                              # 'table_dateType': file_dateType_import_goods,
                                              })
        if not flag:
            return _response
        if worker.import_goods(data_in=data,
                               file_df=pd.read_excel(files.stream), ):
            _response = {"code": 0,
                         "msg": "",
                         }
        else:
            _response = {"code": 103,
                         "msg": "未知原因",
                         }
        return _response

    def query_goods() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_query_goods,
                                                                    'check_exist': check_query_goods},
                                    )
        if flag:
            totalLength, res_df = worker.query_goods(data_in=data)
            _response = {"code": 0,
                         "msg": "",
                         "data": {"total": totalLength, "detail": df_tolist(res_df)}
                         }
        return _response

    def export_goods() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_export_goods,
                                                                    'check_exist': check_export_goods},
                                    )
        if flag:
            _response = {"code": 0,
                         "msg": "",
                         "data": worker.export_goods(data_in=data),
                         }
        return _response

    def set_goods_status() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_offShelf_goods,
                                                                    'check_exist': check_offShelf_goods})
        if flag:
            if worker.set_goods_status(data_in=data):
                _response = {"code": 0,
                             "msg": "", }
            else:
                _response = {"code": 108,
                             "msg": "未能执行删除", }
        return _response

    def upload() -> dict:
        _response = {"code": 0,
                     "msg": "",
                     "data": uploadWorker.editorData(data_in=data, img=files, ),
                     }
        return _response

    def add_activity() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_activity,
                                                                    'check_exist': pre_activity_required})
        if flag:
            flag = activityWorker.addActivity(data_in=data)
            if flag:
                _response = {"code": 106, "msg": "缺少参数"}
            else:
                _response = {"code": 0, "msg": ""}
        return _response

    def query_activity() -> dict:
        totalLength, res_df = activityWorker.getActivity(data_in=data)
        _response = {"code": 0,
                     "msg": "",
                     "data": {"totalLength": totalLength,
                              "detail": df_tolist(res_df), }
                     }
        return _response

    def get_activity_info() -> dict:
        info = activityWorker.getActivityById(data_in=data)
        detail = df_tolist(info)
        _response = {"code": 0,
                     "msg": "",
                     "data": detail[0]
                     }
        return _response

    def edit_activity() -> dict:
        res = activityWorker.editActivityById(data_in=data)
        if res:

            _response = {"code": 0,
                         "msg": ""
                         }

        else:
            _response = {
                "code": -1,
                "msg": "修改失败"
            }
        return _response

    def delete_activity() -> dict:
        res = activityWorker.deleteActivityById(data_in=data)
        if res:

            _response = {"code": 0,
                         "msg": ""
                         }

        else:
            _response = {
                "code": -1,
                "msg": "修改失败"
            }
        return _response

    def add_cart() -> dict:
        _response = shoppingCartWorker.addCart(data_in=data)
        return _response

    def query_cart() -> dict:
        res = shoppingCartWorker.getCartList(data_in=data)
        _response = {"code": 0,
                     "msg": "",
                     "data": df_tolist(res)
                     }
        return _response

    def edit_cart_num() -> dict:
        # 判断是否超过库存，若超过，返回错误信息
        isOverStock = shoppingCartWorker.isOverStock(data_in=data)

        if isOverStock:
            _response = {
                "code": -1,
                "msg": "库存不足"
            }
        else:
            # 若没有超过，修改数量
            res = shoppingCartWorker.changeCartNum(editNum=data.get("num"), ShoppingCartID=data.get("ShoppingCartID"))
            if res:
                _response = {
                    "code": 0,
                    "msg": ""
                }
            else:
                _response = {
                    "code": -1,
                    "msg": "修改购物车数量失败"
                }
        return _response

    def create_order() -> dict:
        res, errMsg = orderWorker.createOrder(data_in=data)
        if res:
            _response = {"code": 0,
                         "msg": ""
                         }
        else:
            _response = {
                "code": -1,
                "msg": errMsg
            }
        return _response

    def query_order() -> dict:
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_query_order,
                                                                    'check_exist': check_query_order,
                                                                    })
        if flag:
            totalLength, res_df = worker.query_order(data_in=data)
            _response = {
                "code": 0,
                "msg": "",
                "data": {
                    "totalLength": totalLength,
                    "detail": df_tolist(res_df)
                }
            }
        return _response

    def confirm_order() -> dict:
        res = orderWorker.confirmOrder(data_in=data)
        if res:
            _response = {
                "code": 0,
                "msg": "",
            }
        else:
            _response = {
                "code": -1,
                "msg": "确定失败，请联系管理员",
            }
        return _response

    def reject_order() -> dict:
        res = orderWorker.rejectOrder(data_in=data)
        if res:
            _response = {
                "code": 0,
                "msg": "",
            }
        else:
            _response = {
                "code": -1,
                "msg": "退回失败，请联系管理员",
            }
        return _response

    def finish_order() -> dict:
        res = orderWorker.finishOrder(data_in=data)
        if res:
            _response = {
                "code": 0,
                "msg": "",
            }
        else:
            _response = {
                "code": -1,
                "msg": "确认领取失败，请联系管理员",
            }
        return _response

    def upload_goodsImage() -> dict:
        pure_data = data.copy()
        pure_data.pop("GoodsCode", '404')
        url = uploadWorker.editorData(data_in=pure_data, img=files, )  # 调用图片上传
        if not worker.upload_goodsImage(data_in=data, image_url=url):
            _response = {"code": 103,
                         "msg": "未知原因",
                         }
        else:
            _response = {"code": 0,
                         "msg": "",
                         "data": url,
                         }
        return _response

    def query_orderDetail():
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_query_orderDetail,
                                                                    'check_exist': check_query_orderDetail,
                                                                    })
        if flag:
            _response = {"code": 0,
                         "msg": "",
                         "data": df_tolist(worker.query_orderDetail(data_in=data))}
        return _response

    def delete_cart() -> dict:
        res = shoppingCartWorker.deleteCart(data_in=data)
        if res:
            _response = {"code": 0,
                         "msg": ""
                         }
        else:
            _response = {
                "code": -1,
                "msg": '删除失败'
            }

        return _response

    def query_FixedPoints():
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_query_FixedPoints,
                                                                    'check_exist': check_query_FixedPoints,
                                                                    })

        if flag:
            totalLength, res_df = worker.query_FixedPoints(data_in=data)
            _response = {"code": 0,
                         "msg": "",
                         "data": {"totalLength": totalLength,
                                  "detail": df_tolist(res_df), }
                         }
        return _response

    def export_FixedPoints():
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_export_FixedPoints,
                                                                    'check_exist': check_export_FixedPoints,
                                                                    })

        if flag:
            res = worker.export_FixedPoints(data_in=data)
            _response = {"code": 0,
                         "msg": "",
                         "data": res
                         }
        return _response

    def query_FixedPoints_ByYear():
        flag, _response = pre_check(data=data, file=files, checker={'check_type': pre_query_FixedPoints_ByYear,
                                                                    'check_exist': check_query_FixedPoints_ByYear,
                                                                    })

        if flag:
            res = worker.query_FixedPoints_ByYear(data_in=data)
            _response = {"code": 0,
                         "msg": "",
                         "data": res
                         }
        return _response

    cdef dict switch = {"query_rewardPoint": query_rewardPoint, "import_rewardPoint": import_rewardPoint,
                        "delete_rewardPoint": delete_rewardPoint, "export_rewardPoint": export_rewardPoint,
                        "account_rewardPoint": account_rewardPoint,
                        "query_RewardPointSummary": query_RewardPointSummary,
                        "export_RewardPointSummary": export_RewardPointSummary,
                        "query_B_RewardPoint": query_B_RewardPoint,
                        "import_goods": import_goods, "query_goods": query_goods, "export_goods": export_goods,
                        "set_goods_status": set_goods_status, "upload": upload, "add_activity": add_activity,
                        "query_activity": query_activity, "get_activity_info": get_activity_info,
                        "edit_activity": edit_activity,
                        "delete_activity": delete_activity, "add_cart": add_cart, "query_cart": query_cart,
                        "edit_cart_num": edit_cart_num, "create_order": create_order, "query_order": query_order,
                        "confirm_order": confirm_order, "reject_order": reject_order, "finish_order": finish_order,
                        "upload_goodsImage": upload_goodsImage,
                        "query_orderDetail": query_orderDetail, "query_FixedPoints": query_FixedPoints,
                        "export_FixedPoints": export_FixedPoints, "query_FixedPoints_ByYear": query_FixedPoints_ByYear,
                        "delete_cart": delete_cart}
    if switch.get(selector):
        return switch.get(selector)()
    else:
        _response = {"code": 9999,
                     "msg": "无效的接口"
                     }
    return _response
def pre_check(dict checker, file, dict data, mustFile=None):
    '''
        用于进行数据校验
    :param checker: 用于检查data内字段
    :param file: flask接收的文件流
    :param data: flask接收的数据
    :param mustFile: 用于输入文件内容检查
    :return:
    '''
    if mustFile is None:
        mustFile = {}
    cdef dict _response = {}
    # 检查输入数据格式 check_type
    for (k, v) in data.items():
        if v == "":
            continue
        if k not in list(checker.get('check_type').keys()):
            _response = {"code": 1, "msg": f"请不要传送无关参数。key:{k},value:{v}"}
            return False, _response
        if not isinstance(v, checker.get('check_type').get(k)):
            _response = {"code": 2, "msg": f"错误参数类型。key:{k},value:{v}，请传送{checker.get('check_type').get(k)}类型"}
            return False, _response
    # 检查必填项 check_exist
    for _exist in checker.get('check_exist'):
        if _exist not in list(data.keys()):
            _response = {"code": 3,
                         "msg": f"缺少必填参数。key:{_exist}，请传送{checker.get('check_type').get(_exist)}类型\n data:{data}"}
            return False, _response
    # 输入文件 check_filetype,table_column
    if file is not None:
        filename, filetype = os.path.splitext(file.filename)
        if not filetype in mustFile.get('check_filetype'):
            _response = {"code": 4, "msg": f"错误文件类型。key:{file.filename}，请传送{mustFile.get('check_filetype')}类型"}
            return False, _response
        if filetype in ('.xlsx', '.xls'):
            temp_df = pd.read_excel(file.stream)
            if temp_df.empty:
                _response = {"code": 5,
                             "msg": f"请勿传送空表"}
                return False, _response
            for i in mustFile.get('table_column'):
                if i not in temp_df.columns.tolist():
                    _response = {"code": 6,
                                 "msg": f"缺少列：{i}，请传送{mustFile.get('table_column')}"}
                    return False, _response
                if temp_df[i].isnull().all():
                    _response = {"code": 7,
                                 "msg": f"请勿传送空列：{i}"}
                    return False, _response
            if mustFile.get('table_dateType'):
                for i in mustFile.get('table_dateType').get('date_column'):
                    for _index in temp_df[i].index.tolist():
                        d_v = temp_df[i][_index]
                        if isinstance(d_v, pd.Timestamp):
                            break
                        _file_dateType_flag = True
                        for j in mustFile.get('table_dateType').get('dateType'):
                            if isVaildDate(date=d_v, timeType=j):
                                _file_dateType_flag = False
                                break
                        if _file_dateType_flag:
                            _response = {"code": 8,
                                         "msg": f"文件中时间类型错误：row:{_index},column:{i}，请传送{mustFile.get('table_dateType').get('dateType')}"}
                            return False, _response

    # 检查参数时间格式 check_dateType
    if checker.get("check_dateType"):
        for i in checker.get("check_dateType"):
            if data.get(i):
                if not isVaildDate(data.get(i)):
                    _response = {"code": 9, "msg": f"错误时间类型,key:{i},value:{data.get(i)}，请传送%Y-%m-%d %H:%M:%S类型"}
                    return False, _response
    return True, _response
