import asyncio
import sys
from threading import Thread

from aiohttp import web
from aiohttp_wsgi import WSGIHandler


def flask_setup(log_level, app_name):
    import logging
    import sys

    import flask
    from flask.logging import default_handler

    log_format = logging.Formatter('%(asctime)s - %(module)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s',
                                   datefmt='%m/%d/%Y %H:%M:%S')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(log_format)
    handler.setLevel(getattr(logging, log_level))

    app = flask.Flask(app_name)
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.DEBUG)
    app.logger.removeHandler(default_handler)

    return app


def run_cycle(cycle):
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.new_event_loop()
    loop.run_until_complete(cycle.run())
    loop.close()


def run_cycle_async(cycle):
    t = Thread(target=run_cycle(cycle))
    t.start()


def make_aiohttp_app(app):
    wsgi_handler = WSGIHandler(app)
    aioapp_ = web.Application()
    aioapp_.router.add_route('*', '/{path_info:.*}', wsgi_handler)
    return aioapp_
