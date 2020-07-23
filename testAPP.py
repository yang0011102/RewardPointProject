import json

from flask import Flask, render_template, request, jsonify, Response, send_from_directory
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer, BadSignature, SignatureExpired
import functools
from config.config import SECRET_KEY

serializer = Serializer(SECRET_KEY, expires_in=3600)


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


app = Flask(__name__, static_url_path='')
app.debug = True

from tool.tool import MyEncoder
@app.route('/get_token/jobid:<jobid>', methods=['GET'])
def get_token(jobid):
    try:
        token=serializer.dumps({'jobid': jobid})
        _response={'token':token}
        return json.dumps(_response,cls=MyEncoder)
    except Exception as e:
        return json.dumps({"args":e.args})



@app.route('/ppp/<name>',methods=['POST'])
@login_required
def _run(name):
    return f"name is {name}"


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
