# -*- coding: utf-8 -*-

import os
import datetime
import collections
import threading

import unqlite
import filelock

import hashlib
import pickle

import sqlite3
import math
import contextlib

from fast_trader.utils import timeit


settings = {
    'disk_store_folder': '~/work/share/cache',
}


class Serializer:

    @staticmethod
    def serialize(obj):
        if isinstance(obj, bytes):
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


class SqliteStore(object):

    # TODO: use sqlalchemy

    def __init__(self, db_name, table_name, fields):

        self._max_length = 1000000000

        self.table_name = table_name
        self.db_name = db_name

        assert 'id' not in [f.lower() for f in fields]
        self.fields = fields

        self._indexed_fields = []
        self._conns = {}

        self._db_initialized = False

    @property
    def _conn(self):

        if not self._db_initialized:
            self._db_initialized = True
            self.assure_table()

        conn_id = (os.getpid(), threading.get_ident())

        if conn_id not in self._conns:
            self._conns[conn_id] = conn = sqlite3.connect(
                self.db_name)

            def dict_factory(cursor, row):
                d = {}
                for idx, col in enumerate(cursor.description):
                    d[col[0]] = row[idx]
                return d

            conn.row_factory = dict_factory

        return self._conns[conn_id]

    def close(self):
        self._conn.commit()
        self._conn.close()

    def assure_table(self, name=None):
        if name is None:
            name = self.table_name

        with contextlib.closing(self._conn.cursor()) as cursor:
            try:
                cursor.execute("SELECT * FROM {} LIMIT 1".format(name))
            except sqlite3.OperationalError as e:
                warnings.warn(str(e) + '. Would reset connection')
                fields_str = ','.join(self.fields)
                fields_str = 'ID INTEGER PRIMARY KEY,' + fields_str
                cursor.execute("CREATE TABLE {} ({})".format(name, fields_str))
                self._conn.commit()

        self.assure_index()

    def reset_table(self, name):
        with contextlib.closing(self._conn.cursor()) as cursor:
            fields_str = ','.join(self.fields)
            fields_str = 'ID INTEGER PRIMARY KEY,' + fields_str
            cursor.execute("CREATE TABLE {} ({})".format(name, fields_str))
            self._conn.commit()

        self.delete({})

    def add_index(self, field):
        if field not in self._indexed_fields:
            self._indexed_fields.append(field)

    def assure_index(self, fields=None):
        if fields is not None:
            for field in fields:
                self.add_index(field)

        for key in self._indexed_fields:
            self._add_index(key)

    def _add_index(self, key):
        with contextlib.closing(self._conn.cursor()) as cursor:
            name = f'{key}_'
            stmt = (f"SELECT * FROM sqlite_master WHERE type ="
                    f" 'index' and tbl_name = '{self.table_name}'"
                    f" and name = '{name}'")

            if cursor.execute(stmt).fetchone() is None:
                cursor.execute(
                    f"CREATE INDEX {name} ON {self.table_name}({key})")
                self._conn.commit()

    def write(self, doc):

        statement = "INSERT INTO {} ({}) VALUES ({})".format(
            self.table_name,
            ','.join(doc.keys()),
            ','.join(['?'] * len(doc))
        )
        try:
            with contextlib.closing(self._conn.cursor()) as cursor:
                cursor.execute(statement, list(doc.values()))
                self._conn.commit()
        except sqlite3.OperationalError as e:
            # reset conn if underlying sqlite gets deleted
            warnings.warn(str(e) + '. Would reset connection')
            self._conns.pop(os.getpid())
            self.assure_table()
            self.write(doc)

    def write_many(self, docs):
        list(map(self.write, docs))

    def read(self, query=None, limit=None):

        statement = "SELECT {} FROM {}".format(
            ','.join(self.fields), self.table_name)
        if query:
            query_str = self._format_condition(query)
            statement += " WHERE {}".format(query_str)

        if limit:
            statement += " ORDER BY ID DESC LIMIT {}".format(limit)

        with contextlib.closing(self._conn.cursor()) as cursor:
            ret = cursor.execute(statement).fetchall()

        return ret

    def read_latest(self, query, by):
        query_str = self._format_condition(query)
        statement = (
            "SELECT {fields} FROM {table} WHERE ID in" +
            "(SELECT MAX(ID) FROM {table} WHERE {con} GROUP BY {by})"
        ).format(
            fields=','.join(self.fields),
            con=query_str,
            table=self.table_name,
            by=by)

        with contextlib.closing(self._conn.cursor()) as cursor:
            ret = cursor.execute(statement).fetchall()

        return ret

    def read_distinct(self, fields):
        with contextlib.closing(self._conn.cursor()) as cursor:
            ret = cursor.execute("SELECT DISTINCT {} FROM {}".format(
                ','.join(fields), self.table_name)).fetchall()
        return ret

    @staticmethod
    def _format_assignment(doc):
        s = str(doc)
        formatted = s[2:-1].replace(
            "': ", '=').replace(", '", ',')
        return formatted

    @staticmethod
    def _format_condition(doc):
        if not doc:
            return 'TRUE'
        s = str(doc)
        formatted = s[2:-1].replace(
            "': ", ' = ').replace(
            ", '", ',').replace(
            "= {'$like =", 'like').replace(
            '}', '')
        return formatted

    def update(self, query, document):

        query_str = self._format_condition(query)
        document_str = self._format_assignment(document)

        statement = "UPDATE {} SET {} WHERE {}".format(
            self.table_name,
            document_str,
            query_str
        )

        with contextlib.closing(self._conn.cursor()) as cursor:
            cursor.execute(statement)
            self._conn.commit()

    def delete(self, query):
        query_str = self._format_condition(query)
        if query_str:
            query_str = f'WHERE {query_str} '
        with contextlib.closing(self._conn.cursor()) as cursor:
            cursor.execute("DELETE FROM {} {}".format(
                self.table_name,
                query_str
            ))
            self._conn.commit()


class SqliteKVStore(object):

    def __init__(self, table_name, sqlite_uri=None):
        self.db_path = sqlite_uri or settings['sqlite-uri']
        self.table_name = table_name
        self._store = SqliteStore(
            self.db_path, table_name, ['key', 'value'])
        self._store.add_index('key')
        self._meta_prefix = '__meta_'

    def read(self, key):
        res = self._store.read({'key': key}, limit=1)
        assert len(res) <= 1

        if len(res) == 0:
            return None

        b_value = res[0]['value']

        if isinstance(b_value, int):  # splited
            b_value = self._read_split_blob(key, b_value)

        return serializer.deserialize(b_value)

    def write(self, key, value):
        b_value = serializer.serialize(value)
        value_len = len(b_value)
        if value_len > self._store._max_length:
            self._split_blob_and_save(value, value_len, key)
        else:
            self._store.write({'key': key, 'value': b_value})

    def _split_blob_and_save(self, blob, length, key):
        number = math.ceil(length / self._store._max_length)
        step = math.ceil(length / number)
        for idx, i in enumerate(range(0, length, step)):
            sub = blob[i:i+step]
            sub_key = f'{key}_{idx}'
            self._store.write({'key': sub_key, 'value': sub})

        self._store.write({'key': key, 'value': number})

    def _read_split_blob(self, key, number):
        sub_keys = [f'{key}_{i}' for i in range(number)]

        def _get_sub(k):
            res = self._store.read({'key': k}, limit=1)
            return res[0]['value']

        blob = b''.join([_get_sub(k) for k in sub_keys])
        return blob

    def has_key(self, key):
        with contextlib.closing(self._store._conn.cursor()) as cursor:
            ret = cursor.execute(f"SELECT key FROM {self.table_name} "
                                 f"WHERE key = '{key}' LIMIT 1").fetchone()
        return ret is not None

    def list_keys(self):
        return sorted([i['key'] for i in self._store.read_distinct(['key'])])

    def delete(self, key):
        self._store.delete({'key': key})

    def read_meta(self, key):
        meta_key = self._meta_prefix + key
        return self.read(meta_key)

    def read_all_meta(self):
        res = self._store.read_latest(
            query={'key': {'$like': '__meta%'}},
            by='key')
        meta = {i['key']: serializer.deserialize(i['value']) for i in res}
        return meta

    def write_meta(self, key, meta):
        meta_key = self._meta_prefix + key
        self.write(meta_key, meta)

    def delete_meta(self, key):
        meta_key = self._meta_prefix + key
        self.delete(meta_key)


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


# class DiskStore_(MemoryStore):

#     def __init__(self, db_uri):
#         super().__init__()

#         self._snapshot = {}
#         # persistent store
#         self._pstore = UnqliteStore(db_uri)
#         self.batch_size = 5

#         self._take_snapshot()

#     def _take_snapshot(self):
#         for key in self._pstore.list_keys():
#             value = self._pstore.read(key)
#             self._snapshot[key] = value[-1]
    
#     def _update_snapshot(self, key, value):
#         self._snapshot[key] = value

#     def list_keys(self):
#         return list(self._snapshot.keys())

#     def write(self, key, value):
#         super().write(key, value)
#         self._update_snapshot(key, value)
#         self.batch_commit(key)

#     def read(self, key):
#         raise NotImplementedError

#     def read_latest(self, keys):
#         ret = [self._snapshot[k] for k in keys]
#         return ret

#     def commit(self, key=None):
#         if key is None:
#             for k in self.list_keys():
#                 self.commit_by_key(k)
#         else:
#             self.commit_by_key(key)
    
#     def commit_by_key(self, key):
#         rec = self._store[key][:]
#         if rec:
#             self._pstore.append(key, rec)
#         self._store[key] = []

#     def batch_commit(self, key):
#         if len(self._store[key]) >= self.batch_size:
#             self.commit_by_key(key)


class DiskStore(MemoryStore):

    def __init__(self, db_uri, writable=False, engine='sqlite'):
        super().__init__()

        self.writable = writable
        self.engine = engine

        self._snapshot = {}
        # persistent store
        if engine == 'unqlite':
            self._pstore = UnqliteStore(db_uri)
        elif engine == 'sqlite':
            self._pstore = SqliteKVStore(
                'quote_feed', db_uri + '.' + engine)
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
                settings['disk_store_folder'], f'unqlite_{self.name}.db'))
            self._store = DiskStore(uri, engine=store_engine, writable=writable)

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
