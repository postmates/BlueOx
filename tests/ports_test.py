import os
from testify import (
    TestCase,
    assert_equal,
    teardown)

from blueox import ports


class DefaultHostTest(TestCase):
    def test_none(self):
        host = ports._default_host(None, 'localhost', 123)

        hostname, port = host.split(':')
        assert_equal(hostname, 'localhost')
        assert_equal(port, '123')

    def test_host(self):
        host = ports._default_host('master', 'localhost', 123)

        hostname, port = host.split(':')
        assert_equal(hostname, 'master')
        assert_equal(port, '123')

    def test_explicit(self):
        host = ports._default_host('master:1234', 'localhost', 123)

        hostname, port = host.split(':')
        assert_equal(hostname, 'master')
        assert_equal(port, '1234')


class DefaultControlHost(TestCase):
    @teardown
    def clear_env(self):
        try:
            del os.environ['BLUEOX_CLIENT_HOST']
        except KeyError:
            pass

    def test_emtpy(self):
        host = ports.default_control_host()
        assert_equal(host, "127.0.0.1:3513")

    def test_env(self):
        os.environ['BLUEOX_CLIENT_HOST'] = 'master'
        host = ports.default_control_host()
        assert_equal(host, "master:3513")

    def test_env_port(self):
        os.environ['BLUEOX_CLIENT_HOST'] = 'master:123'
        host = ports.default_control_host()
        assert_equal(host, "master:123")


class DefaultCollectHost(TestCase):
    @teardown
    def clear_env(self):
        try:
            del os.environ['BLUEOX_HOST']
        except KeyError:
            pass

    def test_emtpy(self):
        host = ports.default_collect_host()
        assert_equal(host, "127.0.0.1:3514")

    def test_env(self):
        os.environ['BLUEOX_HOST'] = 'master'
        host = ports.default_collect_host()
        assert_equal(host, "master:3514")

    def test_env_port(self):
        os.environ['BLUEOX_HOST'] = 'master:123'
        host = ports.default_collect_host()
        assert_equal(host, "master:123")


class DefaultPycernanHost(TestCase):
    @teardown
    def clear_env(self):
        try:
            del os.environ['BLUEOX_PYCERNAN_HOST']
        except KeyError:
            pass

    def test_emtpy(self):
        host = ports.default_pycernan_host()
        assert_equal(host, '127.0.0.1:2003')

    def test_env(self):
        os.environ['BLUEOX_PYCERNAN_HOST'] = 'local.svc.team-me.aws.jk8s'
        host = ports.default_pycernan_host()
        assert_equal(host, 'local.svc.team-me.aws.jk8s:2003')

    def test_env_port(self):
        os.environ['BLUEOX_PYCERNAN_HOST'] = 'local.svc.team-me.aws.jk8s:2003'
        host = ports.default_pycernan_host()
        assert_equal(host, 'local.svc.team-me.aws.jk8s:2003')

    def test_passed(self):
        _host = 'my.wish.is.your.command'
        host = ports.default_pycernan_host(_host)
        assert_equal(host, 'my.wish.is.your.command:2003')
