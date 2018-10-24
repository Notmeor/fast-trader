# -*- coding: utf-8 -*-

import zmq
import random
import time, datetime
import queue
import threading

import pymongo
from dtp import quotation_pb2 as quote_struct

from fast_trader.utils import message2dict, load_config

conf = load_config()


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

        self.url = config['{}_channel'.format(name)]
        self._listeners = []
        self._handlers = []

        self.subscribe('')
        threading.Thread(target=self.start).start()

    def add_listener(self, listener):
        self._listeners.append(listener)

    def on_data(self, data):

        for listener in self._listeners:
            listener.put(Mail(
                api_id='trade_feed',
                api_type='resp',
                content=data
            ))


def to_datetime(n_date, n_time):
    dt = datetime.datetime.strptime(
        '{}{}'.format(n_date, n_time),
        '%Y%m%d%H%M%S%f')
    return dt.astimezone(datetime.timezone.utc)


class Store(object):

    def __init__(self, db_name):

        from influxdb import InfluxDBClient
        from influxdb import DataFrameClient

        config = self.get_config()

        self.client = InfluxDBClient(
            host=config['host'],
            port=config['port'],
            username=config['user'],
            password=config['password'],
            database=db_name
        )

        self.df_client = DataFrameClient(
            host=config['host'],
            port=config['port'],
            username=config['user'],
            password=config['password'],
            database=db_name
        )

        if {'name': db_name} not in self.client.get_list_database():
            self.client.create_database(db_name)

    def get_config(self):
        return {
            'host': 'localhost',
            'port': 8086,
            'user': 'root',
            'password': 'root'
        }

    def write_points(self, points):
        self.client.write_points(points)

    def write_df(self, df):
        self.df_client.write_points(df)

    def query(self, query):
        return self.client.query(query)


class Ticks(MarketFeed):

    url = conf['tick_feed_channel']

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

        self._queue = queue.Queue()

        self._store = Store('test')

        self.subscribe('snapshot(000651)')
        threading.Thread(target=self.start).start()

    def on_data(self, data):

        if not isinstance(data, str):
            self._queue.put(data)
            self._store.write_points([
                {
                    'measurement': 'market_tick',
                    'fields': message2dict(data),
                    'time': to_datetime(data.nActionDay, data.nLocalTime)
                }
            ])

    def get(self):
        return self._queue.get()


class Trades(MarketFeed):

    url = conf['trade_feed_channel']

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._queue = queue.Queue()

        self._store = Store('test')

        self.subscribe('transaction(000651)')
        threading.Thread(target=self.start).start()

    def on_data(self, data):

        if not isinstance(data, str):
            self._queue.put(data)
            self._store.write_points([
                {
                    'measurement': 'market_trade',
                    'fields': message2dict(data),
                    'time': to_datetime(data.nActionDay, data.nLocalTime)
                }
            ])

    def get(self):
        return self._queue.get()


if __name__ == '__main__':
    pass
    ticks = Ticks()
    trades = Trades()





