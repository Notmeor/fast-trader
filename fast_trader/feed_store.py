# -*- coding: utf-8 -*-

import os
import collections


class MemoryStore:

    def __init__(self):
        self._field_mapping = None
        self._store = collections.defaultdict(list)

    def write(self, key, value):
        self._store[key].append(value)

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


class DiskStore:

    def __init__(self):
        raise NotImplementedError

    def write(self, key, value):
        pass

    def read(self, key):
        pass


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

    def __init__(self, datasource_cls, store=None):

        self.name = datasource_cls.name
        self._keep_seconds = 0

        if store is None:
            self._store = MemoryStore()
        else:
            self._store = store

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

    def get_all_codes(self):
        return self._store.list_keys()

    def pull(self, codes=None):
        if codes is None:
            codes = self._store.list_keys()
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

    def get_ticks(self, codes, orient='list'):
        """
        获取历史行情数据

        Parameters
        ----------
        orient: str
            返回数据结构类型
            dict: {code -> [ticks]}
            list; [ticks]
        """

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
