import logging

import numpy as np
from flask import Flask, render_template, request, jsonify, Response, send_from_directory

from config.config import *
from dispatcher import dispatcher
from tool.tool import *


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return datetime_string(pd.to_datetime(obj, "%Y-%m-%d %H:%M:%S"))
        else:
            return super(MyEncoder, self).default(obj)


class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app = Flask(__name__, static_url_path='')
app.jinja_env.auto_reload = True
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['ALLOWED_EXTENSIONS'] = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'xlsx', 'xls'}
app.config['APPLICATION_ROOT'] = APPLICATION_ROOT
app.response_class = MyResponse
logging.basicConfig(filename=f"./log/web.txt", format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@app.route('/')
def _index():
    return render_template('index.html')


@app.route('/2048game')
def _show():
    return render_template('2048.html')


@app.route('/download/<filename>')
def _download(filename):
    print(filename)
    return send_from_directory(DOWNLOAD_FOLDER, filename=filename)


@app.route('/Interface/<selector>', methods=['POST'])
def _inferface(selector):
    print("selector = ", selector)
    data = request.form.to_dict()
    if data == {}:
        data = json.loads(request.get_data(as_text=True))
    print("data:", data)
    file = request.files.get('file')
    if file:
        print("Got a file: ", file)
    else:
        print("file is:", file)
    _response = json.dumps(dispatcher(selector=selector, data=data, files=file), cls=MyEncoder)
    print("返回体：", _response)
    return _response


@app.route('/Interface/test', methods=['POST'])
def __ttt():
    print("GOT")
    data = request.form.to_dict()
    print("Data is:", data)
    file = request.files.get('file')
    if file:
        print("Got a file: ", file)
    else:
        print("file is:", file)
    df = pd.DataFrame(data=[[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=('column1', 'column2', 'column3'),
                      index=('rowA', 'rowB', 'rowC'))
    rrrr = [1, 2, 'wang', 4]
    # return {"data": df.to_json(orient='records', force_ascii=False), }
    return {"data": json.dumps(df_tolist(df)), }
    # return json.dumps({"d": rrrr})


@app.route('/Interface/testfile', methods=['POST'])
def __tttfile():
    print("GOTfile")
    data = request.form.to_dict()
    print(data)
    file = request.files["file"]
    print(file.stream)
    temm = pd.read_excel(file.stream)
    print(temm.columns)
    return "Got your file"
