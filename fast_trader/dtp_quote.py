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
from fast_trader.dtp import quotation_pb2 as quote_struct
from fast_trader.settings import settings
from fast_trader.utils import timeit, message2dict, Mail


class MarketFeed(object):

    url = None

    def __init__(self):
        # self._ctx = zmq_context.CONTEXT
        self._ctx = zmq_context.manager.context
        self._socket = self._ctx.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVHWM, 0)
        self._socket.setsockopt(zmq.RCVBUF, 102400)

        self._queue = queue.Queue()
        self._running = False

        self._consumer = threading.Thread(target=self._retrieve)

    def is_running(self):
        return self._running

    def _parse_data(self, msg):

        if msg.endswith(b')'):
            return msg.decode()

        data = quote_struct.MarketData()
        data.ParseFromString(msg)
        _type = data.Type.Name(data.type).lower()

        return getattr(data, _type)

    def _on_message(self, msg):
        data = self._parse_data(msg)
        if not isinstance(data, str):
            self.on_data(data)

    def sub(self, topic):
        self._socket.subscribe(topic)

    def _retrieve(self):
        while True:
            msg = self._queue.get()
            self._on_message(msg)

    def _recv(self):
        msg = self._socket.recv()
        self._queue.put(msg)

    def _do_sub_in_background(self, codes):
        """
        Calling `socket.subscribe` in a blocking way results
        in only part of desired topics being successfully
        subscribed, especially when there are over 1000 topics.
        This might result from some buffer overload on the
        server side.
        """
        def _subscribe():
            codes_to_subscribe = codes[:]
            while codes_to_subscribe:
                topic = self._to_topic(codes_to_subscribe.pop())
                self.sub(topic)
                time.sleep(0.001)

        _suber = threading.Thread(target=_subscribe)
        _suber.start()

    def _start(self):

        self._socket.connect(self.url)
        self._running = True

        while self._running:
            self._recv()
            while self._socket.getsockopt(zmq.RCVMORE):
                self._recv()

    def on_data(self, data):
        raise NotImplementedError


class QuoteFeed(MarketFeed):

    name = 'undefined'

    def __init__(self, *args, **kw):

        super().__init__(*args, **kw)

        self.url = settings['{}_channel'.format(self.name)]
        self.subscribed_codes = []
        self.subscribed_all = False
        self._listeners = []
        self._handlers = []

        self.logger = logging.getLogger(
            'fast_trader.dtp_quote.{}'.format(self.name))

        self.as_raw_message = False
        self._thread = None

    def start(self):

        if self.subscribed_all:
            self.logger.warning('已订阅全市场{}行情!'.format(self.name))

        if self.is_running():
            return

        self._thread = threading.Thread(target=self._start)
        self._thread.start()

        self._consumer.start()

    def _to_topic(self, code):
        kind = {
            'trade_feed': 'transaction',
            'index_feed': 'index',
            'tick_feed': 'snapshot',
            'order_feed': 'order',
            'queue_feed': 'order_queue',
            'options_feed': 'snapshot',
            'ctp_feed': 'snapshot',
        }[self.name]
        topic = '{}({})'.format(kind, code)
        return topic

    def subscribe(self, code):
        if isinstance(code, str):
            codes = [code]
        else:
            codes = code

        for c in codes:
            if c not in self.subscribed_codes:
                self.subscribed_codes.append(c)
        if codes:
            self._do_sub_in_background(codes)

    def subscribe_all(self):
        self.sub('')
        self.subscribed_all = True

    def add_listener(self, listener):
        self._listeners.append(listener)

    def format(self, data):
        pass
        return data

    def on_data(self, data):

        if not self.as_raw_message:
            data = self.format(data)

        for listener in self._listeners:
            listener.put(Mail(
                api_id=self.name,
                api_type='rsp',
                content=data
            ))


class TradeFeed(QuoteFeed):
    """
    逐笔成交
    """
    name = 'trade_feed'

    def format(self, data):
        ret = message2dict(data)
        price_fields = ['nPrice']
        for field in price_fields:
            ret[field] = ret[field] / 10000
        return ret


class IndexFeed(QuoteFeed):
    """
    指数行情
    """
    name = 'index_feed'

    def format(self, data):
        ret = message2dict(data)
        price_fields = [
            'nOpenIndex', 'nHighIndex', 'nLowIndex',
            'nLastIndex', 'nPreCloseIndex']
        for field in price_fields:
            ret[field] = ret[field] / 10000
        return ret


class TickFeed(QuoteFeed):
    """
    快照行情
    """
    name = 'tick_feed'

    def format(self, data):
        ret = message2dict(data)
        price_fields = [
            'nAskPrice_0', 'nAskPrice_1', 'nAskPrice_2',
            'nAskPrice_3', 'nAskPrice_4', 'nAskPrice_5',
            'nAskPrice_6', 'nAskPrice_7', 'nAskPrice_8',
            'nAskPrice_9', 'nBidPrice_0', 'nBidPrice_1',
            'nBidPrice_2', 'nBidPrice_3', 'nBidPrice_4',
            'nBidPrice_5', 'nBidPrice_6', 'nBidPrice_7',
            'nBidPrice_8', 'nBidPrice_9', 'nHigh',
            'nHighLimited', 'nLow', 'nLowLimited',
            'nMatch', 'nOpen', 'nPreClose',
            'nWeightedAvgAskPrice', 'nWeightedAvgBidPrice']
        for field in price_fields:
            ret[field] = ret[field] / 10000
        return ret


class OrderFeed(QuoteFeed):
    """
    逐笔报单
    """
    name = 'order_feed'

    def format(self, data):
        ret = message2dict(data)
        price_fields = ['nPrice']
        for field in price_fields:
            ret[field] = ret[field] / 10000
        return ret


class QueueFeed(QuoteFeed):
    """
    成交队列
    """
    name = 'queue_feed'

    def format(self, data):
        ret = message2dict(data)
        price_fields = ['nPrice']
        for field in price_fields:
            ret[field] = ret[field] / 10000
        return ret


class OptionsFeed(QuoteFeed):
    """
    期权行情
    """
    name = 'options_feed'

    def format(self, data):
        ret = message2dict(data)
        price_fields = []
        for field in price_fields:
            ret[field] = ret[field] / 10000
        return ret


class FuturesFeed(QuoteFeed):
    """
    期货ctp行情
    """
    name = 'ctp_feed'

    def format(self, data):
        ret = message2dict(data)
        return ret


FEED_TYPE_NAME_MAPPING = {
    'tick_feed': TickFeed,
    'trade_feed': TradeFeed,
    'order_feed': OrderFeed,
    'order_queue': QueueFeed,
    'index_feed': IndexFeed,
    'options_feed': OptionsFeed,
    'ctp_feed': FuturesFeed
}


def get_pb_fields(proto_type):
    return [f.name for f in proto_type.DESCRIPTOR.fields]

Snapshot = namedtuple('Snapshot',
                      get_pb_fields(quote_struct.Stock))

Transaction = namedtuple('Transaction',
                         get_pb_fields(quote_struct.Transaction))

MarketOrder = namedtuple('MarketOrder',
                         get_pb_fields(quote_struct.Order))

Index = namedtuple('Index',
                   get_pb_fields(quote_struct.Index))

OrderQueue = namedtuple('OrderQueue',
                        get_pb_fields(quote_struct.OrderQueue))

OrderQueue = namedtuple('Future',
                        get_pb_fields(quote_struct.Future))
