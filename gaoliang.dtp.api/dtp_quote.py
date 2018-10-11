# -*- coding: utf-8 -*-

import zmq
import random
import time
import queue

import pymongo
from dtp import Quotation_pb2 as quote_struct
 
conf = {
    'IndexFeed': 'tcp://192.168.211.194:7778',
    'TickFeed': 'tcp://192.168.211.194:7779',
    'TradeFeed': 'tcp://192.168.211.194:7781',
    'OrderFeed': 'tcp://192.168.211.194:7782',
    'QueueFeed': 'tcp://192.168.211.194:7783',
}

class Mongo(object):
    
    url = 'mongodb://192.168.211.169:27017'
    
    def __init__(self):
        self._client = pymongo.MongoClient(self.url)        
    
    def __getitem__(self, key):
        return self._client[key]
    
    def close(self):
        self._client.close()
        
    def __enter__(self):
        return self._client
    
    def __exit__(self, et, ev, tb):
        self.close()
    
    def __def__(self):
        self.close()


class MarketFeed(object):

    url = None
    
    def __init__(self):
        self._ctx = zmq.Context()
        self._socket = self._ctx.socket(zmq.SUB)
        self._queue = queue.Queue()
        self._running = False
        
        self._socket.connect(self.url)
    
    @property
    def is_alive(self):
        return self._running
    
    def _parse_data(self, msg):
        if msg.endswith(b')'):
            return msg.decode()
        data = quote_struct.MarketData()
        data.ParseFromString(msg)
        _type = data.Type.Name(data.type).lower()
        return getattr(data, _type)
    
    def subscribe(self, codes):
        self._socket.subscribe(codes)
    
    def start(self):
        
        self._running = True
        
        while self._running:
            ret = self._socket.recv()

            data = self._parse_data(ret)
            self.on_data(data)
            time.sleep(1)
    
    def on_data(self, data):

        print(data)


class OrderFeed(MarketFeed):
    """
    逐笔报单行情
    """
    
    url = conf['OrderFeed']
    
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)


class TradeFeed(MarketFeed):
    """
    逐笔成交行情
    """
    
    url = conf['TradeFeed']
    
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)


class QueueFeed(MarketFeed):
    """
    委托队列
    """
    
    url = conf['QueueFeed']
    
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)


class TickFeed(MarketFeed):
    """
    快照行情
    """
    
    url = conf['TickFeed']
    
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)


class IndexFeed(MarketFeed):
    """
    指数行情
    """
    
    url = conf['IndexFeed']
    
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

if __name__ == '__main__':
    tf = TickFeed()
    tf.subscribe('snapshot(002230.SZ)')
    tf.start()