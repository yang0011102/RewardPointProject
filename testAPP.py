import json

from flask import Flask
from verify import _token,serializer,login_required

app = Flask(__name__, static_url_path='')
app.debug = True


@app.route('/get_token/jobid:<jobid>', methods=['GET'])
def get_token(jobid):
    return _token(jobid=jobid, serializer=serializer)


@app.route('/ppp/<name>', methods=['POST'])
@login_required
def _run(name):
    return f"name is {name}"


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
