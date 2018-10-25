# -*- coding: utf-8 -*-

import zmq
import random
import time, datetime
import queue
import threading

import numpy as np
import pandas as pd

from fast_trader.dtp import quotation_pb2 as quote_struct

from fast_trader.utils import timeit, message2dict, load_config, Mail

conf = load_config()


class MarketFeed(object):

    url = None

    def __init__(self):
        self._ctx = zmq.Context()
        self._socket = self._ctx.socket(zmq.SUB)
        self._queue = queue.Queue()
        self._running = False

    @property
    def is_alive(self):
        return self._running

    def _parse_data(self, msg):

        if msg.endswith(b')'):
            # return msg.decode()
            return

        data = quote_struct.MarketData()
        data.ParseFromString(msg)
        _type = data.Type.Name(data.type).lower()

        return getattr(data, _type)

    def sub(self, topic):
        self._socket.subscribe(topic)

    def _start(self):

        self._socket.connect(self.url)
        self._running = True

        while self._running:
            ret = self._socket.recv()

            data = self._parse_data(ret)
            self.on_data(data)
            # time.sleep(1)

    def on_data(self, data):

        print(data, '\n')


class QuoteFeed(MarketFeed):

    # 逐笔成交
    TradeFeed = 'trade_feed'
    # 指数行情
    IndexFeed = 'index_feed'
    # 快照
    TickFeed = 'tick_feed'
    # 逐笔报单
    OrderFeed = 'order_feed'
    # 成交队列
    QueueFeed = 'queue_feed'

    def __init__(self, name, *args, **kw):

        super().__init__(*args, **kw)

        self.name = name
        self.url = conf['{}_channel'.format(name)]
        self.subscribed_codes = []
        self._listeners = []
        self._handlers = []

    def start(self):
        if not self.subscribed_codes:
            print('当前订阅列表为空!')
            return
        threading.Thread(target=self._start).start()

    def _to_topic(self, code):
        kind = {
            'trade_feed': 'transaction',
            'index_feed': 'index',
            'tick_feed': 'snapshot',
            'order_feed': 'order',
            'queue_feed': 'order_queue'
        }[self.name]
        topic = '{}({})'.format(kind, code)
        return topic

    def subscribe(self, code):

        if isinstance(code, list):
            for c in code:
                self.subscribe(c)
        elif isinstance(code, str):
            self.subscribed_codes.append(code)
            topic = self._to_topic(code)
            self.sub(topic)
        else:
            raise TypeError(
                'Expected `list` or `str`, got `{}`'.format(type(code)))

    def add_listener(self, listener):
        self._listeners.append(listener)

    def on_data(self, data):

        for listener in self._listeners:
            listener.put(Mail(
                api_id=self.name,
                api_type='rsp',
                content=data
            ))







