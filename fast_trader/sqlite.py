import os
import threading
import time
import datetime
import sqlite3
import contextlib
import warnings
import collections
import logging

import math

from fast_trader.serializer import serializer, compress, decompress

LOG = logging.getLogger('fast_trader.feed_store.sqlite')


class SqliteStore(object):

    # TODO: use sqlalchemy

    def __init__(self, db_name, table_name, fields):

        self._max_length = 1000000000

        self.table_name = table_name
        self.db_name = db_name

        assert 'id' not in [f.lower() for f in fields]
        self.fields = fields

        self._indexed_fields = collections.OrderedDict()
        self._conns = {}

        self._db_initialized = False

    @property
    def _conn_id(self):
        return (os.getpid(), threading.get_ident())

    @property
    def _conn(self):

        if not self._db_initialized:
            self._db_initialized = True
            self.assure_table()

        conn_id = self._conn_id

        if conn_id not in self._conns:
            self._conns[conn_id] = conn = sqlite3.connect(
                self.db_name + '.db')

            def dict_factory(cursor, row):
                d = {}
                for idx, col in enumerate(cursor.description):
                    d[col[0]] = row[idx]
                return d

            conn.row_factory = dict_factory

        return self._conns[conn_id]

    def close(self):
        # self._conn.commit()
        self._conns.clear()

    def assure_table(self, name=None):
        if name is None:
            name = self.table_name

        with self._conn:
            try:
                self._conn.execute("SELECT * FROM {} LIMIT 1".format(name))
            except sqlite3.OperationalError:
                fields_str = ','.join(self.fields)
                fields_str = 'ID INTEGER PRIMARY KEY,' + fields_str
                self._conn.execute(
                    "CREATE TABLE {} ({})".format(name, fields_str))

        self.assure_index()

    def reset_table(self, name):
        with self._conn:
            fields_str = ','.join(self.fields)
            fields_str = 'ID INTEGER PRIMARY KEY,' + fields_str
            self._conn.execute(
                "CREATE TABLE {} ({})".format(name, fields_str))

        self.delete({})

    def reset_connection(self, exc=None):
            # reset conn if underlying sqlite gets deleted
            LOG.warning(str(exc) + '. Would reset connection')
            try:
                LOG.error('Try closing busy conn')
                self._conn.close()
            except:
                LOG.error('Close busy conn failed', exc_info=True)
            self._conns.pop(self._conn_id)
            self.assure_table()

    def add_index(self, field, unique=False):
        self._indexed_fields[field] = {'unique': unique}

    def assure_index(self):
        for key, meta in self._indexed_fields.items():
            self._add_index(key, unique=meta['unique'])

    def _add_index(self, key, unique):
        with self._conn:
            name = f'{key}_'
            stmt = (f"SELECT * FROM sqlite_master WHERE type ="
                    f" 'index' and tbl_name = '{self.table_name}'"
                    f" and name = '{name}'")

            if self._conn.execute(stmt).fetchone() is None:
                _unique = 'UNIQUE' if unique else '' 
                self._conn.execute(
                    f"CREATE {_unique} INDEX {name} ON "
                    f"{self.table_name}({key})")

    def write(self, doc):

        statement = "INSERT INTO {} ({}) VALUES ({})".format(
            self.table_name,
            ','.join(doc.keys()),
            ','.join(['?'] * len(doc))
        )

        def _write():
            with self._conn:
                    self._conn.execute(statement, tuple(doc.values()))

        try:
            _write()
        except sqlite3.OperationalError as e:
            self.reset_connection(exc=e)
            _write()

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

        def _read():
            with self._conn:
                ret = self._conn.execute(statement).fetchall()
            return ret

        try:
            return _read()
        except sqlite3.OperationalError as e:
            self.reset_connection(exc=e)
            return _read()

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
        print(statement)
        with self._conn:
            ret = self._conn.execute(statement).fetchall()

        return ret

    def read_distinct(self, fields):
        with self._conn:
            ret = self._conn.execute("SELECT DISTINCT {} FROM {}".format(
                ','.join(fields), self.table_name)).fetchall()
        return ret

    @staticmethod
    def _format_assignment(doc):
        r = []
        for k in doc:
            r.append(f'{k}=?')
        return ','.join(r)

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

        with self._conn:
            self._conn.execute(statement, tuple(document.values()))

    def delete(self, query):
        query_str = self._format_condition(query)
        if query_str:
            query_str = f'WHERE {query_str} '
        with self._conn:
            self._conn.execute("DELETE FROM {} {}".format(
                self.table_name,
                query_str
            ))


class SqliteKVStore(object):

    def __init__(self, table_name, sqlite_uri=None):
        self.db_path = sqlite_uri
        self.table_name = table_name
        self._store = SqliteStore(
            self.db_path, table_name, ['key', 'value'])
        self._store.add_index('key')
        self._meta_prefix = '__meta_'

        self.use_compression = False

    def read(self, key):
        res = self._store.read({'key': key}, limit=1)
        assert len(res) <= 1

        if len(res) == 0:
            return None

        b_value = res[0]['value']

        if isinstance(b_value, int):  # splited
            b_value = self._read_split_blob(key, b_value)

        if self.use_compression:
            b_value = decompress(b_value)

        return serializer.deserialize(b_value)

    def write(self, key, value):
        b_value = serializer.serialize(value)

        if self.use_compression:
            # do not compress metadata
            if not key.startswith(self._meta_prefix):
                b_value = compress(b_value)

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
        with self._store._conn as conn:
            ret = conn.execute(f"SELECT key FROM {self.table_name} "
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
