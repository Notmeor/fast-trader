# -*- coding: utf-8 -*-

import os
import collections


class FieldMap:
    code = 'code'
    date = 'date'
    time = 'time'
    price = 'price'


class MemoryStore:

    def __init__(self, field_map=None):
        self._field_map = field_map
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

    def read_attr(self, item, attr):
        attr_ = self._field_map[attr]
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
        self._code_field = None
        self.store = store
        self.callbacks = []

    def put(self, msg):
        data = msg['content']
        code = getattr(data, self._code_field)
        self.store.write(code, data)
        for cb in self.callbacks:
            cb(data)


class FeedStore:

    def __init__(self, datasource):

        self.store_type = 'memory'
        self._keep_seconds = 0

        if self.store_type == 'memory':
            self._store = MemoryStore()
        else:
            self._store = DiskStore()

        self._listener = Listener(self._store)

        self.datasource = datasource
        self._set_code_field_name()

        self.datasource.add_listener(self._listener)

    def get_all_codes(self):
        return self._store.list_keys()

    def pull(self, codes=None):
        if codes is None:
            codes = self._store.list_keys()
        ret = self._store.read_latest(codes)
        return ret

    def _set_code_field_name(self):
        if self.datasource.name == 'ctp_feed':
            self._listener._code_field = 'code'
        else:
            self._listener._code_field = 'szCode'

    def _get_code_price(self, tick):
        if self.datasource.name == 'ctp_feed':
            return tick['code'], tick['lastPrice']
        return tick['szWindCode'], tick['nMatch']

    def get_current_prices(self, codes=None):
        ticks = self.pull(codes)
        # ret = [self._get_code_price(tick) for tick in ticks]
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

    def add_callback(self, cb):
        self._listener.callbacks.append(cb)


if __name__ == '__main__':

    from fast_trader.dtp_quote import TickFeed, FuturesFeed

    l0 = []
    class QuoteFeed_(TickFeed):
        def on_data(self, data):
            l0.append(data)

    # md = QuoteFeed_()
    ds = FuturesFeed()
    ds.as_raw_message = False
    ds.set_field_mapping({
        'code': 'code',
        'date': 'actionDay',
        'time': 'updateTime',
    })
    # md.subscribe(['10001313'])
    # ds.subscribe_all()
    # ds.subscribe(['002230', '300104'])
    ds.subscribe(['IF1906', 'IC1906'])
    ds.start()

    fs = FeedStore(ds)