# utf-8
from tornado.wsgi import WSGIContainer
from tornado.options import options
from tornado import httpserver, ioloop
from app import app

if __name__ == '__main__':
    options.parse_command_line()
    wsgi_server = httpserver.HTTPServer(WSGIContainer(app))
    wsgi_server.listen(8080)
    print("Here we GOOOOOOOOOOOOOOOOOOOOOOOO!")
    ioloop.IOLoop.current().start()
