# utf-8
from gevent import monkey

monkey.patch_all()
from gevent import pywsgi
from app import app

if __name__ == "__main__":
    app.debug = True
    server = pywsgi.WSGIServer(('192.168.40.229', 8080), app)
    print("Here we GOOOOOOOOOOOOOOOOOOOOOOOO!")
    server.serve_forever()
