#utf-8
import logging

from flask import Flask, render_template, request, jsonify, Response, send_from_directory
from config.config import *
from flask_cors import CORS
from dispatcher_switch import dispatcher
from tool.tool import *


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
logging.basicConfig(filename=f"./log/web.{time.strftime('%Y_%m_%d')}.txt", format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# @app.route('/2048game')
# def _show():
#     return render_template('2048.html')
#
#
# @app.route('/download/<filename>')
# def _download(filename):
#     print(filename)
#     return send_from_directory(DOWNLOAD_FOLDER, filename=filename)


@app.route('/Interface/<selector>', methods=['POST'])
def _inferface(selector):
    data = request.form.to_dict()
    if data == {}:
        data = simplejson.loads(request.get_data(as_text=True))
    print("--------- Data is: ",data)
    file = request.files.get('file')
    _response = simplejson.dumps(dispatcher(selector=selector, data=data, files=file), cls=SuperEncoder,ignore_nan=True)
    return _response

if __name__ =="__main__":
    app.run(host="127.0.0.1",port="5000")


