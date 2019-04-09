# -*- coding: utf-8 -*-

"""
策略历史仓位记录
"""

import os
import threading
import datetime
import sqlite3

import contextlib


class PositionStore:
    """
    PositionStore 接口定义
    """
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
                    'available_quantity': 100,
                    'balance': 200
                },
                ...
            ]
        """
        raise NotImplementedError

    def set_positions(self, positions):
        raise NotImplementedError

    def get_position_by_code(self, code, **kw):
        raise NotImplementedError


class SqliteStore:

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
        # FIXME: use context manager
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

    def read_latest(self, query):
        query_str = self._format_condition(query)
        statement = (
            "SELECT {fields} FROM {table} WHERE ID in" +
            "(SELECT MAX(ID) FROM {table} WHERE {con} GROUP BY code)"
        ).format(
            fields=','.join(self.fields),
            con=query_str,
            table=self.table_name)

        cursor = self._conn.cursor()
        return cursor.execute(statement).fetchall()

    def read_distinct(self, fields):
        cursor = self._conn.cursor()
        ret = cursor.execute("SELECT DISTINCT {} FROM {}".format(
                ','.join(fields), self.table_name))
        return ret

    @staticmethod
    def _format_assignment(doc):
        s = str(doc)
        formatted = s[2:-1].replace(
            "': ", '=').replace(", '", ',')
        return formatted

    @staticmethod
    def _format_condition(doc):
        if isinstance(doc, str):
            return doc
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
                       'balance', 'available_quantity',
                       'cost', 'update_date', 'update_time']

        self._stores = {}

        self.update_available_quantity()

    @property
    def store(self):
        """
        use differenct sqlite connection for each thread
        """
        tid = threading.get_ident()
        # use absolute path
        try:
            db_path = os.path.join(os.path.dirname(__file__), 'sqlite3')
        except NameError:  # so it would work in python shell
            db_path = os.path.join(os.path.realpath(''), 'sqlite3')
        if tid not in self._stores:
            self._stores[tid] = SqliteStore(
                db_name=db_path,
                table_name='positions',
                fields=self.fields)
        return self._stores[tid]

    def read(self, query=None, limit=None):
        self.store.read(query=query, limit=limit)

    def get_positions(self, strategy_id):
        positions = self.store.read_latest({'strategy_id': strategy_id})
        return positions

    def set_positions(self, positions):
        positions = self.store.write_many(positions)

    def get_position_by_code(self, strategy_id, code, exchange=None):
        query = {'strategy_id': strategy_id, 'code': code}
        if exchange is not None:
            query['exchange'] = exchange
        ret = self.store.read(query, limit=1)
        if ret:
            return ret[-1]
        failover = {'code': code, 'exchange': exchange,
                    'cost': None, 'balance': 0,
                    'available_quantity': 0, 'update_date': '19700101',
                    'update_time': '00:00:00'}
        return failover

    def get_all_strategies(self):
        docs = self.store.read_distinct(
            ['strategy_id']).fetchall()
        strategies = [doc['strategy_id'] for doc in docs]
        return strategies

    def update_available_quantity(self, today=None):
        """
        根据日期变更刷新当日可卖出仓位,
        须在每日交易之前刷新
        """
        if today is None:
            today = datetime.date.today().strftime('%Y%m%d')

        for strategy in self.get_all_strategies():
            positions = self.get_positions(strategy)
            for pos in positions:
                if pos['update_date'] < today:
                    pos['available_quantity'] = pos['balance']
                    pos['update_date'] = today
                    pos['update_time'] = datetime.datetime.now().strftime('%H:%M:%S')
            self.set_positions(positions)


from fast_trader.settings import Session, engine
from fast_trader.models import PositionModel


@contextlib.contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class SqlalchemyPositionStore(PositionStore):
    """
    TODO: proper session scope & concurrency safety
    """

    def __init__(self):

        self.fields = ['strategy_id', 'exchange', 'code', 'name',
                       'balance', 'available_quantity',
                       'cost', 'update_date', 'update_time']

        self._stores = {}

        self.update_available_quantity()

    def get_positions(self, strategy_id):
        with contextlib.closing(Session()) as session:
            ret = (
                session
                .query(PositionModel)
                .filter_by(strategy_id=strategy_id)
                .all()
            )
        return ret

    def set_positions(self, positions):

        with session_scope() as session:

            for pos in positions:
                code = pos['code']
                doc = PositionModel.parse(pos)
                r = (
                    session
                    .query(PositionModel)
                    .filter_by(code=code)
                    .update(doc)
                )

                if r == 0:  # upsert
                    session.add(PositionModel.from_msg(doc, parse=False))

            session.commit()

    def get_position_by_code(self, strategy_id, code, exchange=None):
        query = {'strategy_id': strategy_id, 'code': code}
        if exchange is not None:
            query['exchange'] = exchange

        with session_scope() as session:
            ret = session.query(PositionModel).filter_by(**query).all()

        if ret:
            return ret[-1]
        failover = {'code': code, 'exchange': exchange,
                    'cost_price': None, 'balance': 0,
                    'available_quantity': 0, 'update_date': '19700101',
                    'update_time': '00:00:00'}
        return failover

    def get_all_strategies(self):
        with session_scope() as session:
            res = session.query(PositionModel.strategy_id).all()
        strategies = [el[0] for el in res]
        return strategies

    def update_available_quantity(self, today=None):
        """
        根据日期变更刷新当日可卖出仓位,
        须在每日交易之前刷新
        """
        if today is None:
            today = datetime.date.today().strftime('%Y%m%d')

        for strategy in self.get_all_strategies():
            positions = self.get_positions(strategy)
            for pos in positions:
                if pos['update_date'] < today:
                    pos['available_quantity'] = pos['balance']
                    pos['update_date'] = today
                    pos['update_time'] = \
                        datetime.datetime.now().strftime('%H:%M:%S')
            self.set_positions(positions)
