# -*- coding: utf-8 -*-

import zmq
import random
import time, datetime
import queue
import threading

import numpy as np
import pandas as pd

from fast_trader.dtp import Quotation_pb2 as quote_struct
from fast_trader.dtp_quote import MarketFeed
from fast_trader.utils import (timeit, message2dict, load_config, Mail,
                               int2datetime)

conf = load_config()


class Store:

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
                    'time': int2datetime(data.nActionDay, data.nLocalTime)
                }
            ])

    def get(self):
        return self._queue.get()


class Trades(MarketFeed):

    url = conf['trade_feed_channel']

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._queue = list() # queue.Queue()

        self._store = Store('test')

        self.sub('transaction(002230)')
        threading.Thread(target=self._start).start()

    def on_data(self, data):

        if not isinstance(data, str):
            # self._queue.put(data)
            self._queue.append(data)
#            self._store.write_points([
#                {
#                    'measurement': 'market_trade',
#                    'fields': message2dict(data),
#                    'time': int2datetime(data.nActionDay, data.nLocalTime)
#                }
#            ])

    def get(self):
        return self._queue.get()


if __name__ == '__main__':
    pass
    # ticks = Ticks()
    trades = Trades()

