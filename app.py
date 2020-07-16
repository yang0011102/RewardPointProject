# utf-8
import logging

from flask import Flask, render_template, request, jsonify, Response, send_from_directory

from config.config import *
from dispatcher import dispatcher
from tool.tool import *


class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app = Flask(__name__, static_url_path='')
app.debug = True
app.jinja_env.auto_reload = True
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['ALLOWED_EXTENSIONS'] = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'xlsx', 'xls'}
app.config['APPLICATION_ROOT'] = APPLICATION_ROOT
app.response_class = MyResponse
logging.basicConfig(filename=f"./log/web.{time.strftime('%Y_%m_%d')}.txt", format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@app.route('/2048game')
def _show():
    return render_template('2048.html')

@app.route('/hello')
def _show():
    return "hello !"

@app.route('/download/<filename>')
def _download(filename):
    print(filename)
    return send_from_directory(DOWNLOAD_FOLDER, filename=filename)


@app.route('/Interface/<selector>', methods=['POST'])
def _inferface(selector):
    data = request.form.to_dict()
    if data == {}:
        data = json.loads(request.get_data(as_text=True))
    file = request.files.get('file')
    _response = json.dumps(dispatcher(selector=selector, data=data, files=file), cls=MyEncoder)
    return _response


@app.route('/Interface/test')
def __ttt():
    print("GOT")
    count = 0
    while True:
        if count > 10:
            return "fffff"
        time.sleep(10)
        count += 1
if __name__=="__main__":
    app.run()