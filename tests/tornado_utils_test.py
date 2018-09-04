import time
import random
import collections
import traceback
from testify import assert_equal, setup

import tornado.ioloop
import tornado.gen
import tornado.web
import blueox.tornado_utils
from blueox.utils import get_deep

# vendor module. Tornado testing in Testify
import tornado_test


class AsyncHandler(
        blueox.tornado_utils.BlueOxRequestHandlerMixin,
        tornado.web.RequestHandler):
    @blueox.tornado_utils.coroutine
    def get(self):
        loop = self.request.connection.stream.io_loop

        req_id = self.blueox_ctx.id
        blueox.set('async', True)

        result = yield blueox.tornado_utils.AsyncHTTPClient(loop).fetch(
            self.application.test_url)
        assert result.code == 200

        with blueox.Context('.extra'):
            blueox.set('continue_id', req_id)

        self.write("Hello, world")
        self.finish()


class AsyncErrorHandler(
        blueox.tornado_utils.BlueOxRequestHandlerMixin,
        tornado.web.RequestHandler):
    @blueox.tornado_utils.coroutine
    def get(self):
        loop = self.request.connection.stream.io_loop

        _ = yield tornado.gen.Task(loop.add_timeout, time.time()
                                   + random.randint(1, 2))

        raise Exception('hi')

    def write_error(self, status_code, **kwargs):
        if 'exc_info' in kwargs:
            blueox.set('exception', ''.join(
                traceback.format_exception(*kwargs["exc_info"])))

        return super(AsyncErrorHandler, self).write_error(status_code,
                                                          **kwargs)


class AsyncTimeoutHandler(
        blueox.tornado_utils.BlueOxRequestHandlerMixin,
        tornado.web.RequestHandler):
    @blueox.tornado_utils.coroutine
    def get(self):
        loop = self.request.connection.stream.io_loop

        _ = yield tornado.gen.Task(loop.add_timeout, time.time() + 1.0)


class AsyncRecurseTimeoutHandler(
        blueox.tornado_utils.BlueOxRequestHandlerMixin,
        tornado.web.RequestHandler):
    @blueox.tornado_utils.coroutine
    def post(self):
        loop = self.request.connection.stream.io_loop
        http_client = blueox.tornado_utils.AsyncHTTPClient(loop)

        blueox.set("start", True)
        try:
            _ = yield http_client.fetch(self.request.body, request_timeout=0.5)
        except tornado.httpclient.HTTPError:
            self.write("got it")
        else:
            self.write("nope")

        blueox.set("end", True)


class MainHandler(
        blueox.tornado_utils.BlueOxRequestHandlerMixin,
        tornado.web.RequestHandler):
    def get(self):
        blueox.set('async', False)
        self.write("Hello, world")


class SimpleTestCase(tornado_test.AsyncHTTPTestCase):
    @setup
    def setup_bluox(self):
        blueox.configure(None, None, recorder=self.logit)

    @setup
    def setup_log(self):
        self.log_ctx = collections.defaultdict(list)

    @setup
    def build_client(self):
        self.http_client = blueox.tornado_utils.AsyncHTTPClient(self.io_loop)

    def logit(self, ctx):
        self.log_ctx[ctx.id].append(ctx)

    def get_app(self):
        application = tornado.web.Application([
            (r"/", MainHandler),
            (r"/async", AsyncHandler),
            (r"/error", AsyncErrorHandler),
            (r"/timeout", AsyncTimeoutHandler),
            (r"/recurse_timeout", AsyncRecurseTimeoutHandler),
        ])

        application.test_url = self.get_url("/")
        return application

    def test_error(self):
        f = self.http_client.fetch(self.get_url("/error"), self.stop)
        resp = self.wait()

        assert_equal(len(self.log_ctx), 2)

        found_exception = False
        for ctx_list in self.log_ctx.values():
            for ctx in ctx_list:
                if ctx.to_dict()['body'].get('exception'):
                    found_exception = True

        assert found_exception

    def test_timeout_error(self):
        f = self.http_client.fetch(
                self.get_url("/timeout"), self.stop, request_timeout=0.5)
        resp = self.wait()

        assert_equal(len(self.log_ctx), 1)
        ctx = self.log_ctx[self.log_ctx.keys()[0]][0]
        assert_equal(get_deep(ctx.to_dict(), 'body.response.code'), 599)

    def test_recurse_timeout_error(self):
        url = self.get_url("/timeout")
        _ = self.http_client.fetch(self.get_url("/recurse_timeout"), self.stop,
                                   body=url,
                                   method="POST",
                                   request_timeout=1.5)
        resp = self.wait()

        assert_equal(resp.code, 200)
        assert_equal(resp.body, "got it")

        found_timeout = False
        found_request = False
        for ctx_list in self.log_ctx.values():
            for ctx in ctx_list:
                c = ctx.to_dict()
                if (
                        c['type'] == 'request.httpclient' and
                        c['body']['response']['code'] == 599):
                    found_timeout = True

                if c['type'] == 'request' and get_deep(c, 'body.start'):
                    assert get_deep(c, 'body.end')
                    found_request = True

        assert found_timeout
        assert found_request

    def test_context(self):
        self.http_client.fetch(self.get_url("/async"), self.stop)
        resp = self.wait()

        # If everything worked properly, we should have two separate ids,
        # one will have two contexts associated with it.
        # Hopefully it's the right one.
        found_sync = None
        found_async = None
        found_client = 0
        for ctx_list in self.log_ctx.values():
            for ctx in ctx_list:
                if ctx.name == "request" and ctx.to_dict()['body']['async']:
                    assert_equal(len(ctx_list), 3)
                    found_async = ctx
                if (
                        ctx.name == "request" and
                        not ctx.to_dict()['body']['async']):
                    assert_equal(len(ctx_list), 1)
                    found_sync = ctx
                if ctx.name.endswith("httpclient"):
                    found_client += 1

        assert found_async
        assert found_sync
        assert_equal(found_client, 2)

        assert_equal(resp.code, 200)
