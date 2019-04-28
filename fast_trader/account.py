# -*- coding: utf-8 -*-


import enum


class OrderSide(enum.IntEnum):
    BUY = 1
    SELL = 2
    SELLSHORT = 3
    COVER = 4


@enum.unique
class AccountEventType(enum.Enum):

    # 证券交易
    SECURITY_TRANSACTION = 'security transaction'
    # 转股/送股
    STOCK_DIVIDEND = 'stock dividend'
    # 现金分红
    CASH_DIVIDEND = 'cash dividend'
    # 配股
    RIGHTS_OFFERING = 'rights offering'


class AccountEvent:
    header = None
    body = None


class Account:

    account_no = ''

    cash = 0.

    security_value = 0.

    @property
    def total_value(self):
        return self.cash + self.security_value


class Accountant:
    
    def __init__(self, account):
        self._account = account

    def on_security_transaction(self, event):
        self._account.cash = event.body.cash
        self._account.security_value = event.body.security_value

    def on_stock_dividend(self, event):
        pass

    def on_cash_dividend(self, event):
        pass

    def on_rights_offering(self, event):
        pass
