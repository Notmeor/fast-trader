# -*- coding: utf-8 -*-

"""
策略历史仓位记录
"""

import threading
import sqlite3


class PositionStore(object):

    def get_positions(self, strategy_id):
        """
        获取策略历史持仓

        Returns
        ----------
        ret: list
            [
                {
                    'exchange': 2,
                    'code': '000001',
                    'cost': 10.00,
                    'yesterday_long_quantity': 100,
                    'quantity': 200
                },
                ...
            ]
        """
        raise NotImplementedError

    def set_positions(self, strategy_id, positions):
        raise NotImplementedError


class Store(object):

    # TODO: use sqlalchemy

    def __init__(self, db_name, table_name, fields):
        self.table_name = table_name

        assert 'id' not in [f.lower() for f in fields]
        self.fields = fields

        self._conn = sqlite3.connect(db_name + '.db')

        # self._conn.row_factory = sqlite3.Row
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
        self._conn.row_factory = dict_factory

        self.assure_table(table_name)

    def close(self):
        self._conn.commit()
        self._conn.close()

    def __del__(self):
        self.close()

    def assure_table(self, name):
        try:
            cursor = self._conn.cursor()
            cursor.execute("SELECT * FROM {} LIMIT 1".format(name))
        except sqlite3.OperationalError:
            fields_str = ','.join(self.fields)
            fields_str = 'ID INTEGER PRIMARY KEY,' + fields_str
            cursor.execute("CREATE TABLE {} ({})".format(name, fields_str))
            self._conn.commit()

    def write(self, doc):

        cursor = self._conn.cursor()
        statement = "INSERT INTO {} ({}) VALUES ({})".format(
            self.table_name,
            ','.join(doc.keys()),
            ','.join(['?'] * len(doc))
        )
        print(statement)
        cursor.execute(statement, list(doc.values()))
        self._conn.commit()

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

        cursor = self._conn.cursor()
        return cursor.execute(statement).fetchall()

    @staticmethod
    def _format_assignment(doc):
        s = str(doc)
        formatted = s[2:-1].replace(
            "': ", '=').replace(", '", ',')
        return formatted

    @staticmethod
    def _format_condition(doc):
        s = str(doc)
        formatted = s[2:-1].replace(
            "': ", '=').replace(", '", ',').replace(',', ' and ')
        return formatted

    def update(self, query, document):
        cursor = self._conn.cursor()

        query_str = self._format_condition(query)
        document_str = self._format_assignment(document)

        statement = "UPDATE {} SET {} WHERE {}".format(
            self.table_name,
            document_str,
            query_str
        )

        cursor.execute(statement)
        self._conn.commit()

    def delete(self, query):
        raise NotImplementedError


class SqlitePositionStore(PositionStore):

    def __init__(self):

        self.fields = ['strategy_id', 'exchange', 'code', 'name',
                       'quantity', 'cost_price', 'date', 'time']

        self._stores = {}

    @property
    def store(self):
        """
        use differenct sqlite connection for each thread
        """
        tid = threading.get_ident()
        if tid not in self._stores:
            self._stores[tid] = Store(
                db_name='sqlite3',
                table_name='positions',
                fields=self.fields)
        return self._stores[tid]

    def read(self, query=None, limit=None):
        self._store.read(query=query, limit=limit)

    def get_positions(self, strategy_id):
        positions = self.store.read({'strategy_id': strategy_id}, limit=1)
        return positions

    def set_positions(self, positions):
        positions = self._store.write_many(positions)

    def get_position_by_code(self, code, exchange=None):
        query = {'code': code}
        if exchange is not None:
            query['exchange'] = exchange
        ret = self.store.read(query, limit=1)
        if ret:
            return ret[-1]
        failover = {'code': code, 'exchange': exchange, 'cost_price': None,
                    'quantity': 0, 'date': '19700101', 'time': '00:00:00'}
        return failover
