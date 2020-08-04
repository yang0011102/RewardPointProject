import cx_Oracle
import pandas as pd
from config.dbconfig import ncdb as ncDbInfo
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer, BadSignature, SignatureExpired
from config.config import SECRET_KEY
import functools
from flask import request

db_nc = cx_Oracle.connect(
    f'{ncDbInfo["user"]}/{ncDbInfo["password"]}@{ncDbInfo["host"]}:{ncDbInfo["port"]}/{ncDbInfo["db"]}',
    encoding="UTF-8", nencoding="UTF-8")

serializer = Serializer(SECRET_KEY, expires_in=7200)


def login_required(view_func):
    @functools.wraps(view_func)
    def verify_token(*args, **kwargs):
        token = request.headers.get("Authorization")
        if not token:
            _response = {"code": 9998,
                         "msg": "缺失请求头Authorization"
                         }
            return _response
        try:
            serializer.loads(token)
        except BadSignature:
            _response = {"code": 9997,
                         "msg": "token错误"
                         }
            return _response
        except SignatureExpired:
            _response = {"code": 9996,
                         "msg": "token过期"
                         }
            return _response
        return view_func(*args, **kwargs)

    return verify_token


def _token(jobid: str, serializer):
    base_sql = "select count(rownum) as ISEXIST from hi_psnjob right join bd_psndoc on hi_psnjob.pk_psndoc=bd_psndoc.pk_psndoc where hi_psnjob.endflag ='N' and hi_psnjob.ismainjob ='Y' and hi_psnjob.lastflag  ='Y' and bd_psndoc.enablestate =2 and bd_psndoc.code='{}'"
    isexist = pd.read_sql(base_sql.format(jobid), db_nc).loc[0, 'ISEXIST']
    if isexist > 0:
        try:
            token = serializer.dumps({'jobid': jobid})
            return token
        except Exception as e:
            return e.args
    else:
        return '无效的jobid'
