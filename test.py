# utf-8
from flask import Flask, render_template, request, jsonify, Response, send_from_directory
from config.config import *
# from Interface import RewardPointInterface

app = Flask(__name__)
app.debug = True
app.jinja_env.auto_reload = True
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['ALLOWED_EXTENSIONS'] = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'xlsx', 'xls'}


@app.route("/test")
def index():
    return "hello flask"
@app.route('/2048game')
def _show():
    return render_template('2048.html')

if __name__ == '__main__':
    app.run()