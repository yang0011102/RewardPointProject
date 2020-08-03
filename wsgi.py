# utf-8
from tornado.wsgi import WSGIContainer
from tornado import httpserver, ioloop
from app import app

if __name__ == '__main__':
    wsgi_server = httpserver.HTTPServer(WSGIContainer(app))
    wsgi_server.listen(8080)
    ioloop.IOLoop.current().start()
