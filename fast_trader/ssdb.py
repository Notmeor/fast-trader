

import datetime
import collections
import threading
import queue

from fast_trader.dtp_quote import settings
from fast_trader.serializer import serializer


def get_client():
    import pyssdb
    host = settings['quote_feed_store']['ssdb_host']
    port = settings['quote_feed_store']['ssdb_port']
    client = pyssdb.Client(host, port)
    return client


class SSDBHashmapStore:

    def __init__(self, name):
        self._client = get_client()
        self._hname = name

    @property
    def client(self):
        return self._client

    def write(self, key, value):
        b = serializer.serialize(value)
        self.client.hset(self._hname, key, b)

    def read(self, key, value):
        b = self.client.hget(self._hname, key)
        if b is None:
            return b
        ret = serializer.deserialize(b)
        return ret

    def list_keys(self):
        return self.client.hkeys(self._hname, '', '')


class SSDBListStore:
    """
    SSDB `list` store

    key naming: 'category;code;date'
    """

    def __init__(self):

        self._client = get_client()

        try:
            self._write_buffer_size = \
                settings['quote_feed_store']['ssdb_write_buffer_size']
        except KeyError:
            self._write_buffer_size = 0

        self._buffer = queue.Queue()
        self._writer = None
        self._buffer_interval = 10
        self._last_write_time = datetime.datetime.now()
        self._writer_running = True
        self._buffer_write()

    @property
    def client(self):
        return self._client

    def _push(self, key, value):
        b = serializer.serialize(value)
        self.client.qpush(key, b)

    def _push_many(self, key, values, client=None):
        many = [serializer.serialize(v) for v in values]
        if client is None:
            cl = self.client
        else:
            cl = client
        cl.qpush(key, *many)

    def _buffer_write(self):
        if self._write_buffer_size <= 0:
            return

        def _flush():
            client = get_client()
            buf = collections.defaultdict(list)
            while self._writer_running:
                now = datetime.datetime.now()
                if (now - self._last_write_time).seconds > self._buffer_interval:
                    slef._last_write_time = now
                    try:
                        for i in range(self._buffer.qsize()):
                            el = self._buffer.get(block=False)
                            buf[el[0]].append(el[1])
                    except queue.Empty:
                        pass
                    for key in buf:
                        v = buf[key]
                        if len(v) > 0:
                            self._push_many(key, v, client)
                            buf[key] = []

        self._writer = threading.Thread(target=_flush)
        self._writer.start()

    def push(self, key, value):
        if self._write_buffer_size == 0:
            self._push(key, value)
        else:
            self._buffer.put((key, value))

    def read_first(self, key):
        ret = self.client.qfront(key)
        if ret is None:
            return
        ret = serializer.deserialize(ret)
        return ret

    def read_last(self, key):
        ret = self.client.qback(key)
        if ret is None:
            return
        ret = serializer.deserialize(ret)
        return ret

    def read(self, key, offset=0, limit=1000000):
        ret = self.client.qrange(key, offset, limit)
        if ret is None:
            return
        ret = [serializer.deserialize(i) for i in ret]
        return ret

            
