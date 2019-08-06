#!/usr/bin/env python3

import os
import tornado.ioloop
from tornado.httpserver import HTTPServer
from tornado.netutil import bind_unix_socket

from chord_federation_async.app import application

if __name__ == "__main__":
    application.listen(int(os.environ.get("PORT", "5000")))
    # server = HTTPServer(application)
    # server.add_socket(bind_unix_socket(os.environ.get("SOCKET", "/tmp/federation.sock")))
    tornado.ioloop.IOLoop.instance().start()
