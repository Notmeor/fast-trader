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

from fast_trader.utils import timeit, message2dict, load_config, Mail

conf = load_config()


class MarketFeed(object):

    url = None

    def __init__(self):
        self._ctx = zmq_context.CONTEXT
        self._socket = self._ctx.socket(zmq.SUB)
        self._queue = queue.Queue()
        self._running = False

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

    name = 'undefined'

    def __init__(self, *args, **kw):

        super().__init__(*args, **kw)

        self.url = conf['{}_channel'.format(self.name)]
        self.subscribed_codes = []
        self.subscribed_all = False
        self._listeners = []
        self._handlers = []

        self.logger = logging.getLogger(
            'fast_trader.dtp_quote.{}'.format(self.name))
        
        self.as_raw_message = True

    def start(self):

        if not self.subscribed_codes:
            if self.subscribed_all:
                self.logger.warning('已订阅全市场{}行情!'.format(self.name))
            else:
                self.logger.warning('当前订阅列表为空!')
                return

        if self.is_running():
            return

        threading.Thread(target=self._start).start()

    def _to_topic(self, code):
        kind = {
            'trade_feed': 'transaction',
            'index_feed': 'index',
            'tick_feed': 'snapshot',
            'order_feed': 'order',
            'queue_feed': 'order_queue',
            'options_feed': 'snapshot',
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


class IndexFeed(QuoteFeed):
    """
    指数行情
    """
    name = 'index_feed'


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


class QueueFeed(QuoteFeed):
    """
    成交队列
    """
    name = 'queue_feed'


class OptionsFeed(QuoteFeed):
    """
    期权行情
    """
    name = 'options_feed'


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

if __name__ == '__main__':

    l0 = []
    class QuoteFeed_(QuoteFeed):
        name = 'options_feed'
        def on_data(self, data):
            l0.append(data)

    md = QuoteFeed_()
    md.subscribe(['10001313'])
    md.subscribe_all()
    md.start()