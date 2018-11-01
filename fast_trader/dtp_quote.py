# -*- coding: utf-8 -*-

import zmq
import random
import time, datetime
import queue
import threading
import logging

from collections import namedtuple

import numpy as np
import pandas as pd

from fast_trader import zmq_context
from fast_trader.dtp import Quotation_pb2 as quote_struct

from fast_trader.utils import timeit, message2dict, load_config, Mail

conf = load_config()


class MarketFeed(object):

    url = None

    def __init__(self):
        self._ctx = zmq_context.CONTEXT
        self._socket = self._ctx.socket(zmq.SUB)
        self._queue = queue.Queue()
        self._running = False

    @property
    def is_running(self):
        return self._running

    def _parse_data(self, msg):

        if msg.endswith(b')'):
            return msg.decode()

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
            if not isinstance(data, str):
                self.on_data(data)

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

        self.logger = logging.getLogger(
            'fast_trader.dtp_quote.{}'.format(name))

    def start(self):

        if not self.subscribed_codes:
            self.logger.warning('当前订阅列表为空!')
            return

        if self.is_running:
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
            if code in self.subscribed_codes:
                return
            self.subscribed_codes.append(code)
            topic = self._to_topic(code)
            self.sub(topic)
        else:
            raise TypeError(
                'Expected `list` or `str`, got `{}`'.format(type(code)))

    def subscribe_all(self):
        self.sub('')

    def add_listener(self, listener):
        self._listeners.append(listener)

    def on_data(self, data):

        for listener in self._listeners:
            listener.put(Mail(
                api_id=self.name,
                api_type='rsp',
                content=data
            ))


def get_fields(proto_type):
    return [f.name for f in proto_type.DESCRIPTOR.fields]

Snapshot = namedtuple('Snapshot', get_fields(quote_struct.Stock))
Transaction = namedtuple('Transaction', get_fields(quote_struct.Transaction))
MarketOrder = namedtuple('MarketOrder', get_fields(quote_struct.Order))
Index = namedtuple('Index', get_fields(quote_struct.Index))
