# -*- coding: utf-8 -*-

import time
import datetime
import enum
import collections
import contextlib
import copy
import pandas as pd

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Column, Integer, Float, Boolean, Enum
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.sqlite import SqliteStore
from fast_trader.settings import settings
from fast_trader.utils import timeit, as_wind_code

Base = declarative_base()


class LedgerCategory:
    # 证券持仓价值
    SECURITY = 'security'
    # 可用现金价值
    CASH = 'cash'
    # 冻结现金价值
    FREEZE = 'freeze'


class LedgerSubject:
    # 资本变化（出入金）
    CAPITAL = 'capital'
    # 交易
    TRANSACTION = 'transaction'
    # 费用
    COSTS = 'costs'
    # 估值
    EVALUATION = 'evaluation'
    # 权益事件(分红送转)
    DIVIDEND = 'dividend'
    # 配股
    RIGHTS_OFFERING = 'rights_offering'


class DateAttrMixin:

    @property
    def date(self):
        return self.localtime[:10]


class StockLedgerRecord(Base, DateAttrMixin):

    __tablename__ = 'stock_ledger_record'

    id = Column(Integer, primary_key=True)
    # LedgerSubject
    # subject = Column(Enum(LedgerSubject))
    subject = Column(String(15))
    # LedgerCategory
    # subject = Column(Enum(LedgerCategory))
    category = Column(String(8))
    # 标的代码
    code = Column(String(16))
    # 价格
    price = Column(Float)
    # 数量
    quantity = Column(Float)
    # 本地时间
    localtime = Column(String(32))
    # 备注
    comment = Column(String(50))

    @classmethod
    def from_msg(cls, msg):
        ret = cls()
        for k, v in msg.items():
            if k in cls.__table__.columns:
                setattr(ret, k, v)
        return ret


class AccountView:
    __slots__ = ('cash', 'freeze',
                 'security_value', 'costs')

    def __init__(self):

        # 可用资金
        self.cash = 0.
        # 冻结资金
        self.freeze = 0.
        # 证券市值
        self.security_value = 0.
        # 费用
        self.costs = 0.

    @property
    def balance(self):
        """
        余额
        """
        return self.cash + self.freeze + self.security_value

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__slots__}

    def __repr__(self):
        r = ''
        for field in list(self.__slots__) + ['balance']:
            if not field.startswith('_'):
                r += f'{field}: {getattr(self, field)}\n'
        return r


def ledger_record_to_dict(record):
    dct = record.__dict__
    dct.pop('_sa_instance_state')
    return dct


class HoldingPosition(DateAttrMixin):
    __slots__ = ('code', 'quantity', 'yd_quantity',
                 'price', 'localtime')

    def __init__(self, **kw):
        self.code = ''
        # 当前持仓数量
        self.quantity = 0
        # 可用昨日持仓数量
        self.yd_quantity = 0
        # 持仓价格
        self.price = 0.
        # 最新更新时间
        self.localtime = '1970-01-01T00:00:00.000000'

        for k, v in kw.items():
            setattr(self, k, v)

    @property
    def sellable_quantity(self):
        today = datetime.date.today().strftime('%Y-%m-%d')
        if today > self.date:
            return self.quantity
        return self.yd_quantity

    def __repr__(self):
        return '\n'.join((f'{k}: {getattr(self, k)}'
                          for k in self.__slots__))


class LedgerStore:

    def __init__(self, name):
        self._store = SqliteStore(
            db_name=settings['sqlite_ledger'],
            table_name=name,
            fields=['subject', 'category', 'code', 'price',
                    'quantity', 'localtime', 'comment']
        )

    def save(self, record):
        doc = record.__dict__
        doc.pop('_sa_instance_state')
        self._store.write(doc)

    def load(self, query=None):
        if query is None:
            query = {}
        return self._store.read(query)


class Accountant:

    # TODO: memoize

    def __init__(self, name):
        self.name = name

        ddict = collections.defaultdict
        self._positions = ddict(HoldingPosition)

        # self._general_account_view = AccountView()
        self._account_view_per_child = ddict(AccountView)
        # TODO: history records as namedtuple
        self._account_views_per_child = ddict(dict)

        self._unhandled = 0

        self._store = LedgerStore(name=name)

        self._records = self._load_history()

    @contextlib.contextmanager
    def _get_view(self, record):
        c, t = record.code, record.localtime
        view = self._account_view_per_child[c]
        yield view
        # take snapshot
        self._account_views_per_child[c][t] = copy.copy(view)

    def _load_history(self):
        """
        加载历史流水记录
        """
        # TODO: allow loading from recent records
        docs = self._store.load()
        records = list(map(StockLedgerRecord.from_msg, docs))
        for event in records:
            self.handle_event(event)
        return records

    def save(self, record):
        # TODO: save in background
        # Do not save EVALUATION record
        if record.subject == LedgerSubject.EVALUATION:
            return
        self._store.save(record)

    def put_event(self, event):
        if (not self._records) or event.localtime >=\
                self._records[-1].localtime:
            self._records.append(event)

            self.save(event)
            self.handle_event(event)
            self._unhandled += 1
        else:
            raise RuntimeError('Got outdated ledger record')
            # # allow insertion of missing events
            # length = len(self._records)
            # for i in range(length):
            #     if i == length - 2 or\
            #             event.localtime >= self._records[-i-2].localtime:
            #        self._records.insert(-i-1, event)
            #        break

    def handle_event(self, event):
        # TODO: incremental calc
        if event.subject == LedgerSubject.TRANSACTION:
            self.on_security_transaction(event)
        elif event.subject == LedgerSubject.EVALUATION:
            self.on_evaluation(event)
        elif event.subject == LedgerSubject.DIVIDEND:
            if event.category == LedgerCategory.CASH:
                self.on_cash_dividend(event)
            elif event.category == LedgerCategory.SECURITY:
                self.on_stock_dividend(event)
        elif event.subject == LedgerSubject.RIGHTS_OFFERING:
            self.on_rights_offering(event)
        elif event.subject == LedgerSubject.COSTS:
            self.on_costs(event)
        elif event.subject == LedgerSubject.CAPITAL:
            self.on_capital_change(event)

    def get_cash_by_dataframe(self):
        records = list(map(lambda o: o.__dict__, self._records))
        df = pd.DataFrame(records)
        df = df.drop('_sa_instance_state', axis=1)
        cash_df = df[df.category == LedgerCategory.CASH]
        cash_ss = cash_df.groupby('localtime').apply(
            lambda x: (x.quantity * x.price).sum()
        ).cumsum()

        if not isinstance(cash_ss.index, pd.DatetimeIndex):
            cash_ss.index = pd.to_datetime(cash_ss.index)

        return cash_ss

    def get_holding_value_by_dataframe(self):
        records = list(map(lambda o: o.__dict__, self._records))
        df = pd.DataFrame(records)
        df = df.drop('_sa_instance_state', axis=1)
        holding_df = df[df.category == LedgerCategory.SECURITY]\
            .set_index('localtime')
        common_index = holding_df.index.unique()
        # TODO: multiple trades might occur at the same time,
        # thus resulting in duplicated index
        holding_value_by_code = holding_df.groupby('code').apply(
            lambda x: (x.quantity.cumsum() * x.price)
                # .groupby(x.index).last()  # drop duplicates by index
                .reindex(common_index)
                .fillna(method='ffill')
        )
        holding_ss = holding_value_by_code.sum()

        if not isinstance(holding_ss.index, pd.DatetimeIndex):
            holding_ss.index = pd.to_datetime(holding_ss.index)

        return holding_ss

    def get_profit_by_dataframe(self):
        cash_ss = self.get_cash_by_dataframe()
        holding_ss = self.get_holding_value_by_dataframe()
        co_index = cash_ss.index.union(holding_ss.index)
        
        profit = cash_ss.reindex(co_index, method='ffill').fillna(0.)\
            + holding_ss.reindex(co_index, method='ffill').fillna(0.)
        return profit

    def get_account_view_by_code(self, code):
        return self._account_view_per_child[code]

    def get_general_account_view(self):
        gview = AccountView()
        for view in self._account_view_per_child.values():
            gview.cash += view.cash
            gview.security_value += view.security_value
            gview.costs += view.costs
            gview.freeze += view.freeze
        return gview

    def get_account_history_stats_by_code(self, code, as_dataframe=True):

        views = self._account_views_per_child[code]
        if as_dataframe:
            docs = {k: v.to_dict() for k, v in views.items()}
            df = pd.DataFrame(docs).T
            return df
        return views

    def get_general_account_history_stats(self):

        dataframes = []
        common_index = None
        for code in self._account_views_per_child:
            stats = self.get_account_history_stats_by_code(code)
            dataframes.append(stats)
            if common_index is None:
                common_index = stats.index
            else:
                common_index = common_index.union(stats.index)

        def _fill(x):
            return x.reindex(common_index, method='ffill').fillna(0.)

        dataframes = list(map(_fill, dataframes))

        ret = sum(dataframes)
        return ret

    def on_evaluation(self, event):
        """
        Re-evalute account according to latest market price

        Quantity of evaluation record should always equal 0
        so as not to fail the audit
        """
        pos = self._positions[event.code]
        pre_price = pos.price
        quantity = pos.quantity
        cur_price = event.price

        pos.price = event.price

        # calc evaluation increment
        increment = (cur_price - pre_price) * quantity

        with self._get_view(event) as view:
            view.security_value += increment
            # print(f'\r{event.code}\n: {view}', end='')

    def on_security_transaction(self, event):

        with self._get_view(event) as view:

            if event.category == LedgerCategory.CASH:

                view.cash += event.quantity * event.price

            elif event.category == LedgerCategory.SECURITY:

                pos = self._positions[event.code]

                # 更新剩余历史交易日持仓量
                if event.date > pos.date:
                    pos.yd_quantity = pos.quantity
                    pos.localtime = event.localtime

                if event.quantity < 0:
                    pos.yd_quantity += event.quantity
                    pos.localtime = event.localtime

                # evaluation increment
                eval_inc = (event.price - pos.price) * pos.quantity
                # position value increment
                pos_inc = event.price * event.quantity

                view.security_value += eval_inc + pos_inc

                pos.price = event.price
                pos.quantity += event.quantity

            elif event.category == LedgerCategory.FREEZE:

                view.freeze += event.quantity * event.price

    def on_capital_change(self, event):
        with self._get_view(event) as view:
            view.cash += event.price * event.quantity

    def on_stock_dividend(self, event):

        value = event.quantity * event.price

        with self._get_view(event) as view:
            view.security_value += value

    def on_cash_dividend(self, event):

        value = event.quantity * event.price

        with self._get_view(event) as view:
            view.cash += value

    def on_rights_offering(self, event):
        raise NotImplementedError

    def on_costs(self, event):
        # subtract from cash and add to costs
        value = event.quantity * event.price

        with self._get_view(event) as view:
            view.cash += value
            view.costs += -value


class LedgerWriter:

    def __init__(self, name):
        self.name = name
        self.accountant = Accountant(name)

    @property
    def localtime(self):
        return datetime.datetime.now()

    def write_order_record(self, order):
        """
        委托相关流水

        资金在freeze于cash之间的划转记录
        """
        code = as_wind_code(order.code)
        localtime = self.localtime.strftime('%Y-%m-%dT%H:%M:%S.%f')

        # 委托本地提交后，冻结资金
        if order.status == dtp_type.ORDER_STATUS_SUBMITTED:
            # 冻结开仓委托占用资金
            if order.order_side == dtp_type.ORDER_SIDE_BUY:
                code = as_wind_code(order.code)
                localtime = self.localtime.strftime('%Y-%m-%dT%H:%M:%S.%f')
                frozen_value = float(order.price) * order.quantity

                record = StockLedgerRecord()
                record.subject = LedgerSubject.TRANSACTION
                record.category = LedgerCategory.FREEZE
                record.code = code
                record.quantity = frozen_value
                record.price = 1.
                record.localtime = localtime
                self.accountant.put_event(record)

                record = StockLedgerRecord()
                record.subject = LedgerSubject.TRANSACTION
                record.category = LedgerCategory.CASH
                record.code = code
                record.quantity = -frozen_value
                record.price = 1.
                record.localtime = localtime
                self.accountant.put_event(record)

        # 委托本地提交后，仅冻结了买入委托占用资金
        # 委托确认后，还须冻结交易费用
        # FIXME: 可能不会有`PLACED`状态推送？

        # 缺少已申报回报，则无法预先冻结交易费用，使得成交后冻结资金成为负值，
        # 而可用资金也会等额增多。可在盘后增加一笔划转记录，
        # 将多出的CASH转出，冲销掉FREEZE的负值
        # 或者将（负冻结 + CASH）作为实际可用资金
        elif order.status == dtp_type.ORDER_STATUS_PLACED:
            if order.order_side == dtp_type.ORDER_SIDE_BUY:
                order_freeze = order.price * order.quantity
            else:
                order_freeze = 0.
            cost_freeze = order.freeze_amount - order_freeze

            record = StockLedgerRecord()
            record.subject = LedgerSubject.TRANSACTION
            record.category = LedgerCategory.FREEZE
            record.code = code
            record.quantity = cost_freeze
            record.price = 1.
            record.localtime = localtime
            self.accountant.put_event(record)

            record = StockLedgerRecord()
            record.subject = LedgerSubject.TRANSACTION
            record.category = LedgerCategory.CASH
            record.code = code
            record.quantity = -cost_freeze
            record.price = 1.
            record.localtime = localtime
            self.accountant.put_event(record)

        # 委托撤销后（包含部成部撤），释放资金
        elif order.status == dtp_type.ORDER_STATUS_CANCELLED:
            # 开仓撤单：委托冻结 + 费用冻结 -> 可用资金
            # 平仓撤单：费用冻结 -> 可用资金

            record = StockLedgerRecord()
            record.subject = LedgerSubject.TRANSACTION
            record.category = LedgerCategory.FREEZE
            record.code = code
            record.quantity = order.freeze_amount
            record.price = 1.
            record.localtime = localtime
            self.accountant.put_event(record)

            record = StockLedgerRecord()
            record.subject = LedgerSubject.TRANSACTION
            record.category = LedgerCategory.CASH
            record.code = code
            record.quantity = -order.freeze_amount
            record.price = 1.
            record.localtime = localtime
            self.accountant.put_event(record)

        # 废单，释放资金
        elif order.status == dtp_type.ORDER_STATUS_FAILED:
            # 开仓废单：委托冻结 -> 可用资金
            # 平仓废单：无
            # FIXME: 交易所拒单？是否已经冻结了交易费用？
            if order.order_side == dtp_type.ORDER_SIDE_BUY:

                unfreeze = order.price * order.quantity

                record = StockLedgerRecord()
                record.subject = LedgerSubject.TRANSACTION
                record.category = LedgerCategory.FREEZE
                record.code = code
                record.quantity = -unfreeze
                record.price = 1.
                record.localtime = localtime
                self.accountant.put_event(record)

                record = StockLedgerRecord()
                record.subject = LedgerSubject.TRANSACTION
                record.category = LedgerCategory.CASH
                record.code = code
                record.quantity = unfreeze
                record.price = 1.
                record.localtime = localtime
                self.accountant.put_event(record)

    def write_trade_record(self, trade):
        """
        成交相关流水

        资金在cash于security_value之间的划转，以及交易费用记录
        """
        code = as_wind_code(trade.code)
        localtime = self.localtime.strftime('%Y-%m-%dT%H:%M:%S.%f')
        _sign = -1 if trade.order_side == dtp_type.ORDER_SIDE_BUY else 1
        clear_amount = abs(trade.clear_amount)
        cur_cost = abs(clear_amount - trade.fill_amount)

        # 冻结资金解冻 -> 持仓市值 + 可用资金 + 交易费用
        # 买入委托解冻的资金包括委托占用和费用冻结
        # 卖出委托解冻的只有费用冻结
        if trade.order_side == dtp_type.ORDER_SIDE_BUY:
            unfreeze = trade.price * trade.fill_quantity + cur_cost
        else:
            unfreeze = cur_cost

        record = StockLedgerRecord()
        record.subject = LedgerSubject.TRANSACTION
        record.category = LedgerCategory.FREEZE
        record.code = code
        record.quantity = -unfreeze
        record.price = 1.
        record.localtime = localtime
        self.accountant.put_event(record)

        # 持仓金额变动
        record = StockLedgerRecord()
        record.subject = LedgerSubject.TRANSACTION
        record.category = LedgerCategory.SECURITY
        record.code = code
        record.quantity = trade.fill_quantity * -_sign
        record.price = trade.fill_price
        record.localtime = localtime
        self.accountant.put_event(record)

        # 交易费用变动
        record = StockLedgerRecord()
        record.subject = LedgerSubject.COSTS
        record.category = LedgerCategory.CASH
        record.code = code
        record.quantity = -cur_cost
        record.price = 1.
        record.localtime = localtime
        self.accountant.put_event(record)

        # 可用资金变动 <- 解冻资金 - 持仓市值增值 - 交易费用
        # 成交价可能会优于委托价
        overpayment = abs(trade.price - trade.fill_price) * trade.fill_quantity
        record = StockLedgerRecord()
        record.subject = LedgerSubject.TRANSACTION
        record.category = LedgerCategory.CASH
        record.code = code
        record.quantity = overpayment + cur_cost
        record.price = 1.
        record.localtime = localtime
        record.comment = 'overpayment&cost'
        self.accountant.put_event(record)

    def write_non_trading_activity_record(self, event):
        """
        非交易行为流水

        主要包括个股的分红送转、配股等记录
        """
        raise NotImplementedError

    def write_market_record(self, message):
        """
        估值行情记录
        """
        data = message['content']
        code = data.szWindCode

        if message.api_id == 'trade_feed':
            price = data.nPirce
        elif message.api_id == 'tick_feed':
            price = data.nMatch
        else:
            raise RuntimeError(f'Invalid quote data: {message}')

        localtime = self.localtime.strftime('%Y-%m-%dT%H:%M:%S.%f')

        record = StockLedgerRecord()
        record.subject = LedgerSubject.EVALUATION
        record.category = LedgerCategory.SECURITY
        record.code = code
        record.quantity = 0.
        record.price = price
        record.localtime = localtime
        self.accountant.put_event(record)

    def write_capital_change_record(self, value):
        """
        账户资本金(出入金)记录
        """
        localtime = self.localtime.strftime('%Y-%m-%dT%H:%M:%S.%f')

        record = StockLedgerRecord()
        record.subject = LedgerSubject.CAPITAL
        record.category = LedgerCategory.CASH
        record.code = 'account'
        record.quantity = value
        record.price = 1.
        record.localtime = localtime
        self.accountant.put_event(record)
