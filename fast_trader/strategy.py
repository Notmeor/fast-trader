# -*- coding: utf-8 -*-

import datetime, time

import threading

from fast_trader.dtp_trade import Trader, Mail
from fast_trader.dtp_quote import QuoteFeed, conf

from fast_trader.dtp import dtp_api_id as dtp_api_id
from fast_trader.dtp import api_pb2 as dtp_struct
from fast_trader.dtp import type_pb2 as dtp_type

from fast_trader.dtp import ext_api_pb2 as dtp_struct_
from fast_trader.dtp import ext_type_pb2 as dtp_type_

from fast_trader.utils import timeit, message2dict, load_config


config = load_config()


class Strategy(object):

    def __init__(self, *args, **kw):

        self.trader = Trader()

        self._positions = {}

        self._account = None

        self.subscribed_datasource = []

    def start(self):

        self.trader.start()

        self.trader.add_strategy(self)

        self.trader.login(account=config['account'],
                          password=config['password'])

    @property
    def position(self):
        return {p['code']: p['balance'] for p in self.trader._positions
                if p.get('balance', 0) != 0}

    @timeit
    def get_position(self):
        mail = self.trader.query_position(sync=True)
        payload = mail['content']
        msg = message2dict(payload.body)
        position = self._positions = msg['position_list']
        return position

    @timeit
    def get_account(self):
        mail = self.trader.query_capital(sync=True)
        payload = mail['content']
        return message2dict(payload.body)

    def _get_exchange(self, code):
        if code.startswith('6'):
            return dtp_type.EXCHANGE_SH_A
        else:
            return dtp_type.EXCHANGE_SZ_A

    def add_datasource(self, datasource):

        name = datasource.name
        handler = {
           'market_trade': self.on_market_trade,
           'tick': self.on_tick,
           'market_queue': self.on_market_queue,
           'market_order': self.on_market_order
        }[name]
        self.trader.dispatcher.bind('{}_resp'.format(name), handler)
        datasource.add_listener(self.dispatcher)

        self.subcribed_datasources.append(datasource)

    def on_market_trade(self, market_trade):
        print('market_trade:', market_trade)

    def on_tick(self, tick):
        print('tick:', tick)

    def on_market_queue(self, market_queue):
        print('market_queue:', market_queue)

    def on_market_order(self, market_order):
        print('market_order:', market_order)

    def on_trade(self, trade):
        """
        成交回报
        """
        pass

    def on_order(self, order):
        """
        订单回报
        """
        pass

    def on_order_query(self, orders):
        pass

    def on_trade_query(self, trades):
        pass

    def on_position_query(self, position):
        print('position:', position)

    def on_capital_query(self, account):
        """
        账户查询回报
        """
        pass

    def on_compliance_report(self, report):
        """
        风控回报
        """
        pass

    def buy(self, code, price, quantity):
        exchange = self._get_exchange(code)
        price = str(price)
        self.trader.send_order(
            exchange=exchange, code=code,
            price=price, quantity=quantity,
            order_side=dtp_type.ORDER_SIDE_BUY)

    def sell(self, code, price, quantity):
        exchange = self._get_exchange(code)
        price = str(price)
        self.trader.send_order(
            exchange=exchange, code=code,
            price=price, quantity=quantity,
            order_side=dtp_type.ORDER_SIDE_SELL)


if __name__ == '__main__':

    ea = Strategy()
    ea.start()

