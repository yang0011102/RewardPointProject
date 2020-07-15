# utf-8
'''
用于对输入数据进行预处理
'''
# 查询积分明细
pre_query_rewardPointDetail = {"name": str, "jobid": int, "isBonus": int, "isAccounted": int,
                               "beginDate": str, "endDate": str,
                               "page": int, "pageSize": int,
                               "rewardPointsType": str, }
check_query_rewardPointDetail = []  # 必填
check_dateType_rewardPointDetail = ["beginDate", "endDate"]  # 时间检查项

# 导入积分明细
pre_import_rewardPoint = {"Operator": str}
import_rewardPoint_columncheck = ['工号', '一级部门', '二级部门', '三级部门', '职务名称', 'A分/B分', '加分', '减分', '加减分理由', '加减分依据',
                                  '理由分类', '职能部门/所在部门管理', '提交部门', '考核日期', ]
filetype_rewardPoint = ['.xlsx', '.xls']  # 文件类型检查项
file_dateType_rewardPoint = {'date_column': ['考核日期'],
                             'dateType': ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H",
                                          "%Y-%m-%d", "%Y/%m/%d %H:%M:%S"]}  # 文件内时间字段检查项
check_import_rewardPoint = ["Operator"]  # 必填

# 无效化积分明细
pre_delete_rewardPoint = {"RewardPointsdetailID": int, }
check_delete_rewardPoint = ["RewardPointsdetailID"]  # 必填

# 导出积分明细
pre_export_rewardPoint = {"name": str, "jobid": int, "isBonus": int, "isAccounted": int,
                          'rewardPointsType': str,
                          "Operator": int}
check_export_rewardPoint = ["Operator"]  # 必填

# 结算积分明细
pre_account_rewardPoint = {"RewardPointsdetailID": str, "jobid": str, }
check_account_rewardPoint = []  # 必填

# 积分汇总查询
pre_query_RewardPointSummary = {"name": str, "jobid": int,
                                "page": int, "pageSize": int, }
check_query_RewardPointSummary = []  # 必填

# 积分汇总导出
pre_export_RewardPointSummary={"name": str, "jobid": int,
                                "page": int, "pageSize": int,"Operator": int }
check_export_RewardPointSummary = ["Operator"]  # 必填

# 查询商品信息
pre_query_goods = {"Name": str, "GoodsCode": int, "page": int, "pageSize": int}
check_query_goods = []  # 必填

# 导入商品
pre_import_goods = {"Operator": int}
filetype_import_goods = ['.xlsx', '.xls']  # 文件类型检查项
import_goods_columncheck = ['商品编码', '商品名称', '商品单价', '商品计量单位', '数量']
check_import_goods = ["Operator"]  # 必填
# file_dateType_import_goods = {'date_column': ['进库时间'],
#                               'dateType': ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H",
#                                            "%Y-%m-%d", "%Y/%m/%d %H:%M:%S"]}  # 文件内时间字段检查项
# 导出商品
pre_export_goods = {"Name": str, "GoodsCode": int, "page": int, "pageSize": int,
                    "Operator": int}
check_export_goods = ["Operator"]  # 必填

# 商品变更
pre_offShelf_goods = {"GoodsCode": str,"Status":int}
check_offShelf_goods = ["GoodsCode"]  # 必填

# 上传商品图片
pre_upload_goodsImage = {"GoodsCode": str, 'Operator': int}
check_upload_goodsImage = ["GoodsCode", "Operator"]  # 必填
imagetype_upload_goodsImage = ['png', 'jpg', 'jpeg', 'gif']  # 图片类型检查项
# 新增活动
pre_activity = {
    "Title": str,
    "Slogan": str,
    "PictureUrl": str,
    "BeginDateTime": str,
    "EndDateTime": str,
    "ActivityContent": str,
    "RewardAndPenalties": str,
    "CreatedBy": str
}
pre_activity_required = ["Title", "PictureUrl", "BeginDateTime", "EndDateTime", "ActivityContent", "CreatedBy"]
