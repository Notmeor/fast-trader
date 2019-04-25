# -*- coding: utf-8 -*-

import os
import datetime
import collections
import threading
import warnings

import sqlite3
import math
import contextlib
import zmq

from fast_trader.utils import timeit
from fast_trader.dtp_quote import settings
from fast_trader.sqlite import SqliteKVStore


class MemoryStore:

    def __init__(self):
        self._keep_history = False
        self._field_mapping = None
        self._store = collections.defaultdict(list)

    def write(self, key, value):
        lst = self._store[key]
        if self._keep_history:
            lst.append(value)
        else:
            if lst:
                lst[0] = value
            else:
                lst.append(value)

    def read(self, key):
        return self._store[key]

    def read_latest(self, keys):
        ret = []
        for key in keys:
            if self._store[key]:
                ret.append(self._store[key][-1])
        return ret

    def list_keys(self):
        return list(self._store.keys())

    def remove_expired(self):
        pass

    def set_field_mapping(self, mapping):
        self._field_mapping = mapping

    def read_attr(self, item, attr):
        attr_ = self._field_mapping[attr]
        return getattr(item, attr_)


class DiskStore(MemoryStore):

    def __init__(self, db_uri, writable=False, engine='sqlite'):
        super().__init__()

        self._keep_history = True

        self.writable = writable
        self.engine = engine

        self._snapshot = {}

        if engine == 'sqlite':
            self._pstore = SqliteKVStore('quote_feed', db_uri)
        else:
            raise TypeError(f'Unsupported db engine `{engine}`')

        # commit interval in seconds
        self.commit_interval = 10

        self._take_snapshot()

        self._last_time = datetime.datetime.now()

    def _take_snapshot(self):
        keys = self._pstore.list_keys()
        if keys:
            last_ts = max(keys)
            rec = self._pstore.read(last_ts)
            for k, v in rec.items():
                self._snapshot[k] = v[-1]
    
    def _update_snapshot(self, key, value):
        self._snapshot[key] = value

    @staticmethod
    def ts2dt(ts):
        return datetime.datetime.strptime(ts, '%Y%m%d %H:%M:%S.%f')

    def list_keys(self):
        return list(self._snapshot.keys())

    def write(self, key, value):
        super().write(key, value)
        self._update_snapshot(key, value)
        self.batch_commit()

    def read(self, code):
        raise NotImplementedError

    def read_many(self, codes, from_dt=None):
        keys = sorted(self._pstore.list_keys())
        ret = collections.defaultdict(list)
        for k in keys:
            if from_dt is None or from_dt < self.ts2dt(k):
                rec = self._pstore.read(k)
                for code in codes:
                    ret[code].extend(rec[code])
        return ret

    def read_latest(self, keys):
        ret = [self._snapshot[k] for k in keys]
        return ret

    def commit(self):
        if self.writable:
            ts = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S.%f')
            rec = self._store
            self._pstore.write(ts, rec)
        self._store.clear()

    def batch_commit(self):
        now = datetime.datetime.now()
        if (now - self._last_time).seconds >= self.commit_interval:
            self.commit()
            self._last_time = now


class Listener:

    def __init__(self, store):
        self.store = store
        self.callbacks = collections.OrderedDict()

    def put(self, msg):
        data = msg['content']
        code = self.store.read_attr(data, 'code')
        self.store.write(code, data)
        for cb in self.callbacks.values():
            cb(data)


class FeedStore:

    def __init__(self, datasource_cls,
                 store_type='memory',
                 store_engine='unqlite',
                 writable=False):
        """

        Parameters
        ----------
        store_type: str
            历史数据存储类型
                'memory': store in meory
                'disk': store in disk
        
        writable: bool
            store读写模式，仅对`disk_store`有效
                False: read only
                True: read & write
        """
        self.name = datasource_cls.name
        self._keep_seconds = 0

        self.store_type = store_type
        self.store_engine = store_engine
        self.writable = writable

        if store_type == 'memory':
            self._store = MemoryStore()
        elif store_type == 'disk':
            uri = os.path.expanduser(os.path.join(
                settings['quote_feed_store']['disk_store_folder'],
                f'{self.name}.{store_engine}'))
            self._store = DiskStore(uri, engine=store_engine, writable=writable)
        elif store_type == 'ssdb':
            raise NotImplementedError

        self._listener = Listener(self._store)
        self.datasource = datasource_cls()
        self.datasource.as_raw_message = False
        self.datasource.add_listener(self._listener)

    def set_field_mapping(self, mapping):
        self._store.set_field_mapping(mapping)

    def subscribe(self, codes):
        codes_ = [c.split('.', 1)[0] for c in codes]
        self.datasource.subscribe(codes_)
    
    def subscribe_all(self):
        self.datasource.subscribe_all()

    def connect(self):
        self.datasource.start()

    @staticmethod
    def as_wind_code(code):
        if '.' in code:
            return code
        if code.startswith('6'):
            return code + '.SH'
        else:
            return code + '.SZ'

    def get_all_codes(self):
        # if self.datasource.subscribed_all:
        #     return self._store.list_keys()
        # codes = [self.as_wind_code(c) 
        #          for c in self.datasource.subscribed_codes]
        # return codes
        return self._store.list_keys()

    def pull(self, codes=None):
        """
        获取已订阅标的最新数据
        """
        if codes is None:
            codes = self.get_all_codes()
        ret = self._store.read_latest(codes)
        return ret

    def get_current_prices(self, codes=None):
        ticks = self.pull(codes)
        ret = []
        for tick in ticks:
            code = self._store.read_attr(tick, 'code')
            price = self._store.read_attr(tick, 'price')
            ret.append((code, price))
        return ret

    def get_ticks_(self, codes, from_dt, orient='list'):
        """
        获取历史行情数据

        Parameters
        ----------
        orient: str
            返回数据结构类型
            dict: {code -> [ticks]}
            list; [ticks]
        """
        if not isinstance(codes, list):
            raise TypeError(f'`codes` expects type `list`, got {type(codes)}')

        keys = self._store.list_keys()

        ret = [] if orient == 'list' else {}
        for code in codes:
            if code not in keys:
                continue
            data = self._store.read(code)
            if data is not None:
                if orient == 'list':
                    ret.extend(data)
                else:
                    ret[code] = data
        return ret

    def get_ticks(self, codes, from_dt, orient='list'):
        """
        获取历史行情数据

        Parameters
        ----------
        orient: str
            返回数据结构类型
            dict: {code -> [ticks]}
            list; [ticks]
        """
        if self.store_type == 'memory':
            return self.get_ticks_(codes, from_dt, orient)
        else:
            rec = self._store.read_many(codes, from_dt)
            if orient == 'list':
                ret = sum(rec.values(), [])
                return ret
            else:
                return rec

    @property
    def callbacks(self):
        return self._listener.callbacks

    def add_callback(self, cb, name=None):
        """
        添加回调

        同名函数会相互覆盖
        """
        if name is None:
            name = cb.__name__
        self._listener.callbacks[name] = cb

    def remove_callback(self, cb_or_name):
        if callable(cb_or_name):
            name = cb_or_name.__name__
        else:
            name = cb_or_name
        self._listener.callbacks.pop(name)


class FeedPortal:
    """
    行情数据聚合接口 
    """

    def __init__(self):
        self._stores = {}

    def add_store(self, store):
        self._stores[store.name] = store

    def connect(self):
        for store in self._stores.values():
            store.connect()

    def get_current_prices(self, codes):
        ret = []
        for store in self._stores.values():
            _codes = set(store.get_all_codes()).intersection(codes)
            ret.extend(store.get_current_prices(_codes))
        return ret


class _SaveQuote:

    def __init__(self, store, source_name):
        self.source_name = source_name
        self.store = store
        self._field_mapping = {
            'code': 'szWindCode',
            'date': 'nActionDay',
        }
    
    def set_field_mapping(self, mapping):
        self._field_mapping.update(mapping)

    def put(self, msg):
        data = msg['content']
        code = getattr(data, self._field_mapping['code'])
        date = getattr(data, self._field_mapping['date'])
        key = f'{self.source_name};{code};{date}'
        self.store.push(key, data)


class QuoteCollector:
    """
    实时行情存储
    """

    def __init__(self):
        self._datasources = []
        self._poller = zmq.Poller()
        from fast_trader.ssdb import SSDBListStore
        self._store = SSDBListStore()
        self.__ds_sock_mapping = {}
        self._working_thread = None

        self._consumer = None
    
    @property
    def store(self):
        return self._store
    
    def add_datasource(self, ds):
        self._datasources.append(ds)
        ds.as_raw_message = False

        handler = _SaveQuote(self._store, ds.name)
        if ds.name in ['ctp_feed', 'options_feed']:
            handler.set_field_mapping({
                'code': 'code',
                'date': 'actionDay'
            })
        ds.add_listener(handler)
    
    def is_running(self):
        return self._working_thread.is_alive()
    
    def _receive(self):
        while True:
            try:
                events = dict(self._poller.poll())
            except zmq.ZMQError as e:
                print(e)
                break

            for sock in events:
                ds = self.__ds_sock_mapping[sock]
                ds._recv()
                while ds._socket.getsockopt(zmq.RCVMORE):
                    ds._recv()

    def start(self):
        for ds in self._datasources:
            ds._socket.connect(ds.url)
            self._poller.register(ds._socket)
            self.__ds_sock_mapping[ds._socket] = ds

            ds._consumer.start()
        
        self._working_thread = threading.Thread(target=self._receive)
        self._working_thread.start()


if __name__ == '__main__':

    from fast_trader.dtp_quote import TickFeed, FuturesFeed

    ctp_store = FeedStore(FuturesFeed)
    ctp_store.set_field_mapping({
        'code': 'code',
        'price': 'lastPrice',
        'date': 'actionDay',
        'time': 'updateTime',
    })
    ctp_store.subscribe(['IF1906', 'IC1906'])

    stock_tick_store = FeedStore(TickFeed)
    stock_tick_store.set_field_mapping({
        'code': 'szWindCode',
        'price': 'nMatch',
        'date': 'nActionDay',
        'time': 'nTime',
    })
    stock_tick_store.subscribe(['000001.SZ', '300014.SZ'])

    fp = FeedPortal()
    fp.add_store(ctp_store)
    fp.add_store(stock_tick_store)
    fp.connect()
