# -*- coding: utf-8 -*-

import os
import collections

import unqlite
import filelock

import hashlib
import pickle


settings = {
    'disk_store_folder': '~/work/share/cache',
}


class Serializer:

    @staticmethod
    def serialize(obj):
        if isinstance(obj, (bytes, str)):
            return obj
        if isinstance(obj, str):
            return obj.encode()
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize(b):
        if not isinstance(b, bytes):
            return b
        try:
            return b.decode()
        except:
            return pickle.loads(b)

    @classmethod
    def gen_md5(cls, b, value=False):
        bytes_ = cls.serialize(b)
        md5 = hashlib.md5(bytes_).hexdigest()
        if value:
            return md5, bytes_
        return md5

serializer = Serializer


class UnqliteMode:

    UNQLITE_OPEN_READONLY = 0x00000001
    UNQLITE_OPEN_READWRITE = 0x00000002
    UNQLITE_OPEN_CREATE = 0x00000004
    UNQLITE_OPEN_EXCLUSIVE = 0x00000008
    UNQLITE_OPEN_TEMP_DB = 0x00000010
    UNQLITE_OPEN_NOMUTEX = 0x00000020
    UNQLITE_OPEN_OMIT_JOURNALING = 0x00000040
    UNQLITE_OPEN_IN_MEMORY = 0x00000080
    UNQLITE_OPEN_MMAP = 0x00000100


class UnqliteStore:

    def __init__(self, uri, mode=UnqliteMode.UNQLITE_OPEN_CREATE):
        self._uri = uri
        self._mode = mode
        self._conns = {}

        dirname = os.path.dirname(self._uri)
        db_name = os.path.basename(self._uri)
        self._lock = filelock.FileLock(
            os.path.join(dirname, f'{db_name}.lock'))
    
    @property
    def _conn(self):
        conn_id = os.getpid()
        if conn_id not in self._conns:
            self._conns[conn_id] = unqlite.UnQLite(
                self._uri, flags=self._mode)
        return self._conns[conn_id]
    
    def _write(self, key, value):
        v = serializer.serialize(value)
        with self._lock, self._conn:
            with self._conn.transaction():
                self._conn[key] = v
    
    def _read(self, key):
        with self._lock, self._conn:
            value = self._conn[key]
        v = serializer.deserialize(value)
        return v
    
    def _delete(self, key):
        with self._lock, self._conn:
            self._conn.delete(key)
    
    def _update(self, key, value):
        v = serializer.serialize(value)
        with self._lock, self._conn:
            self._conn.update({key: v})
    
    def has_key(self, key):
        with self._lock, self._conn:
            return self._conn.exists(key)
    
    def list_keys(self):
        with self._lock, self._conn:
            return list(self._conn.keys())
    
    def write(self, key, value):
        self._write(key, value)
    
    def read(self, key):
        try:
            return self._read(key)
        except KeyError:
            return None

    def delete(self, key):
        self._delete(key)
    
    def update(self, key, value):
        self._update(key, value)

    def append(self, key, value):
        old = self.read(key)
        if old is not None:
            new = old + value
        else:
            new = value
        self.update(key, new)


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


class DiskStore(MemoryStore):

    def __init__(self, db_uri):
        super().__init__()

        self._snapshot = {}
        # persistent store
        self._pstore = UnqliteStore(db_uri)
        self.batch_size = 5

        self._take_snapshot()

    def _take_snapshot(self):
        for key in self._pstore.list_keys():
            value = self._pstore.read(key)
            self._snapshot[key] = value[-1]
    
    def _update_snapshot(self, key, value):
        self._snapshot[key] = value

    def list_keys(self):
        return list(self._snapshot.keys())

    def write(self, key, value):
        super().write(key, value)
        self._update_snapshot(key, value)
        self.batch_commit(key)

    def read(self, key):
        raise NotImplementedError

    def read_latest(self, keys):
        ret = [self._snapshot[k] for k in keys]
        return ret

    def commit(self, key=None):
        if key is None:
            for k in self.list_keys():
                self.commit_by_key(k)
        else:
            self.commit_by_key(key)
    
    def commit_by_key(self, key):
        rec = self._store[key][:]
        if rec:
            self._pstore.append(key, rec)
        self._store[key] = []

    def batch_commit(self, key):
        if len(self._store[key]) >= self.batch_size:
            self.commit_by_key(key)


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

    def __init__(self, datasource_cls, store_type='memory'):
        """

        Parameters
        ----------
        store_type: str
            历史数据存储类型
            - 'memory': store in meory
            - 'disk': store in disk
        """
        self.name = datasource_cls.name
        self._keep_seconds = 0

        if store_type == 'memory':
            self._store = MemoryStore()
        elif store_type == 'disk':
            uri = os.path.expanduser(os.path.join(
                settings['disk_store_folder'], f'unqlite_{self.name}.db'))
            print(uri)
            self._store = DiskStore(uri)

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
        if self.datasource.subscribed_all:
            return self._store.list_keys()
        return self.datasource.subscribed_codes

    def pull(self, codes=None):
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
