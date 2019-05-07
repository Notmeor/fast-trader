# -*- coding: utf-8 -*-

import time
import enum
import collections
import contextlib
import copy
import pandas as pd

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Column, Integer, Float, Boolean, Enum
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from fast_trader.sqlite import SqliteStore
from fast_trader.utils import timeit

Base = declarative_base()


#@enum.unique
#class LedgerCategory(enum.Enum):
#    # 证券持仓价值
#    SECURITY = 'security'
#    # 可用现金价值
#    CASH = 'cash'
#    # 冻结现金价值
#    FREEZE = 'freeze'
#
#
#@enum.unique
#class LedgerSubject(enum.Enum):
#    # 交易
#    TRANSACTION = 'transaction'
#    # 费用
#    COSTS = 'costs'
#    # 估值
#    EVALUATION = 'evaluation'
#    # 权益事件(分红送转)
#    DIVIDEND = 'dividend'
#    # 配股
#    RIGHTS_OFFERING = 'rights_offering'


class LedgerCategory:
    # 证券持仓价值
    SECURITY = 'security'
    # 可用现金价值
    CASH = 'cash'
    # 冻结现金价值
    FREEZE = 'freeze'


class LedgerSubject:
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


class StockLedgerRecord(Base):

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


class OrderSide(enum.IntEnum):
    BUY = 1
    SELL = 2
    SELLSHORT = 3
    COVER = 4


@enum.unique
class AccountEventType(enum.Enum):

    # 证券交易
    SECURITY_TRANSACTION = 'security transaction'
    # 费用
    COSTS = 'costs'
    # 转股/送股
    STOCK_DIVIDEND = 'stock dividend'
    # 现金分红
    CASH_DIVIDEND = 'cash dividend'
    # 配股
    RIGHTS_OFFERING = 'rights offering'


class EventHeader:
    event_type: str

class EventBody:
    pass

class AccountEvent:
    header: EventHeader
    body: EventBody

    def __init__(self):
        self.header = EventHeader()
        self.body = EventBody()


engine = None
Session = None


def config_sqlalchemy():
    global engine
    global Session
    uri = 'sqlite:////Users/eisenheim/Documents/git/fast-trader/tmp/ledger.db'
    engine = create_engine(uri)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)


config_sqlalchemy()


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


class HoldingPosition:
    __slots__ = ('code', 'quantity', 'price')

    def __init__(self, **kw):
        self.code = ''
        self.quantity = 0.
        self.price = 0.

        for k, v in kw.items():
            setattr(self, k, v)


class LedgerStore:

    def __init__(self):
        self._store = SqliteStore(
            db_name='test_ledger1.db',
            table_name='stock_ledger',
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

    # TODO: persistence
    # TODO: memoize

    def __init__(self, account):
        self._account = account
        self._records = []

        ddict = collections.defaultdict
        self._positions = ddict(HoldingPosition)

        self._general_account_view = AccountView()
        self._account_view_per_child = ddict(AccountView)
        # TODO: history records as namedtuple
        self._account_views_per_child = ddict(dict)

        self._unhandled = 0
        
        self._store = LedgerStore()

    @contextlib.contextmanager
    def get_view(self, record):
        c, t = record.code, record.localtime
        view = self._account_view_per_child[c]
        yield view
        # take snapshot
        self._account_views_per_child[c][t] = copy.copy(view)


    def put_event(self, event):
        if (not self._records) or event.localtime >=\
                self._records[-1].localtime:
            self._records.append(event)

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

        # self._cash_dict[event.localtime] = self._cash
        # self._holding_dict[event.localtime] = self._holding
        # self._costs_dict[event.localtime] = self._costs

    def _handle_event(self):
        # TODO: incremental calc
        for event in self._records:
            self.handle_event(event)

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
        profit = cash_ss + holding_ss
        return profit
    
    def get_account_view_by_code(self, code):
        return self._account_view_per_child[code]
    
    def get_general_account_view(self):
        gview = self._general_account_view
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
        
        _fill = lambda x: x.reindex(common_index, method='ffill').fillna(0.)
        dataframes = list(map(_fill, dataframes))

        ret = sum(dataframes)
        return ret


    def get_general_cash_records(self):
        pass

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
        
        with self.get_view(event) as view:
            view.security_value += increment

    def on_security_transaction(self, event):

        with self.get_view(event) as view:

            if event.category == LedgerCategory.CASH:

                view.cash += event.quantity * event.price

            if event.category == LedgerCategory.SECURITY:

                pos = self._positions[event.code]

                # evaluation increment
                eval_inc = (event.price - pos.price) * pos.quantity
                # position value increment
                pos_inc = event.price * event.quantity

                view.security_value += eval_inc + pos_inc

                pos.price = event.price
                pos.quantity += event.quantity

    def on_stock_dividend(self, event):

        value = event.quantity * event.price

        with self.get_view(event) as view:
            view.security_value += value

    def on_cash_dividend(self, event):
        
        value = event.quantity * event.price

        with self.get_view(event) as view:
            view.cash += value

    def on_rights_offering(self, event):
        raise NotImplementedError

    def on_costs(self, event):
        # subtract from cash and add to costs
        value = event.quantity * event.price

        with self.get_view(event) as view:
            view.cash += value
            view.costs += -value


