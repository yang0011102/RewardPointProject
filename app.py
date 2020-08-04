# utf-8
import logging

from flask import Flask, request, jsonify, Response

from InterfaceModules.dd import DDInterface
from config.config import *
from config.dbconfig import *
from flask_cors import CORS
from dispatcher_switch import dispatcher
from tool.tool import *
from verify import _token, serializer, login_required


class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app = Flask(__name__, static_url_path='')
CORS(app, supports_credentials=True)  # FLASK跨域
app.jinja_env.auto_reload = True
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['ALLOWED_EXTENSIONS'] = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'xlsx', 'xls'}
app.config['APPLICATION_ROOT'] = APPLICATION_ROOT
app.response_class = MyResponse
logging.basicConfig(filename=f"./log/web.{time.strftime('%Y_%m_%d')}.txt",
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ddWorker = DDInterface(mssqlDbInfo=mssqldb, ncDbInfo=ncdb)


@app.route('/getUserInfo', methods=['POST'])
def getUserInfo() -> dict:
    data = request.form.to_dict()
    if data == {}:
        data = simplejson.loads(request.get_data(as_text=True))
    res = ddWorker.getUserInfo(data_in=data)
    _response = {"code": 0,
                 "msg": "",
                 "data": res,
                 "token": _token(jobid=res.get("jobnumber"), serializer=serializer)
                 }

    return simplejson.dumps(_response, cls=SuperEncoder, ignore_nan=True)


@app.route('/Interface/<selector>', methods=['POST'])
@login_required
def _inferface(selector):
    data = request.form.to_dict()
    if data == {}:
        data = simplejson.loads(request.get_data(as_text=True))
    file = request.files.get('file')
    _response = simplejson.dumps(dispatcher(selector=selector, data=data, files=file), cls=SuperEncoder,
                                 ignore_nan=True)
    return _response


if __name__ == "__main__":
    app.run(host="127.0.0.1", port="5000")
