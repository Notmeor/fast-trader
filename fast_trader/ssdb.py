from fast_trader.dtp_quote import conf
from fast_trader.serializer import serializer 


def get_client():
    import pyssdb
    host = conf['quote_feed_store']['ssdb_host']
    port = conf['quote_feed_store']['ssdb_port']
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

    def __init__(self, *args, **kw):

        self._client = get_client()

    @property
    def client(self):
        return self._client
    
    def push(self, key, value):
        b = serializer.serialize(value)
        self.client.qpush(key, b)

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

            
