# -*- coding: utf-8 -*-

import datetime, time

import threading
import logging

from fast_trader.dtp_trade import Trader, Dispatcher
from fast_trader.dtp_quote import QuoteFeed, conf

from fast_trader.dtp import dtp_api_id as dtp_api_id
from fast_trader.dtp import api_pb2 as dtp_struct
from fast_trader.dtp import type_pb2 as dtp_type

from fast_trader.dtp import ext_api_pb2 as dtp_struct_
from fast_trader.dtp import ext_type_pb2 as dtp_type_

from fast_trader.utils import timeit, message2dict, load_config, Mail


config = load_config()


class Strategy(object):

    def __init__(self, *args, **kw):

        self._positions = {}
        self._orders = {}
        self._trades = {}

        self._account = None

        self.subscribed_datasources = []
        
        self.logger = logging.getLogger('fast_trader.strategy')

    def set_dispatcher(self, dispatcher):
        self.dispatcher = Dispatcher()

    def set_trader(self, trader):
        self.trader = trader

    def start(self):
        
        self.on_start()

        # 启动行情线程
        self.start_market()

        self.trader.add_strategy(self)
        self.trader.start()
        self.trader.login(account=config['account'],
                          password=config['password'])
        
        self.logger.info('策略启动')

    def start_market(self):

        for ds in self.subscribed_datasources:
            ds.start()

    @property
    def position(self):
        return {p['code']: p['balance'] for p in self._positions
                if p.get('balance', 0) != 0}

    @timeit
    def get_position(self):
        mail = self.trader.query_position(sync=True)
        payload = mail['content']
        msg = message2dict(payload.body)
        position = self._positions = msg['position_list']
        return position

    @timeit
    def get_orders(self):
        mail = self.trader.query_orders(sync=True)
        payload = mail['content']
        msg = message2dict(payload.body)
        orders = self._orders = msg['order_list']
        return orders

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
           'trade_feed': self.on_market_trade,
           'tick_feed': self.on_tick,
           'queue_feed': self.on_market_queue,
           'order_feed': self.on_market_order,
           'index_feed': self.on_market_index
        }[name]

        dispatcher = self.dispatcher
        datasource.add_listener(dispatcher)
        dispatcher.bind('{}_rsp'.format(name), handler)

        self.subscribed_datasources.append(datasource)

    def on_start(self):
        pass

    def on_market_trade(self, market_trade):
        print('market_trade')
        pass

    def on_tick(self, tick):
        print('tick')

    def on_market_queue(self, market_queue):
        print('market_queue')

    def on_market_order(self, market_order):
        print('market_order')

    def on_market_index(self, market_index):
        print('market_index')

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

    def on_batch_order_submission(self, msg):
        print(msg)

    def on_order_query(self, orders):
        pass

    def on_order_cancelation_submission(self, msg):
        print(msg)

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

    def cancel_order(self, exchange, order_exchange_id):
        self.trader.cancel_order(exchange, order_exchange_id)


def get_strategy_instance(StrategyCls):

    dispatcher = Dispatcher()

    trader = Trader(dispatcher)

    strategy = StrategyCls()

    strategy.set_dispatcher(dispatcher)
    strategy.set_trader(trader)

    return strategy


if __name__ == '__main__':

    dispatcher = Dispatcher()

    trader = Trader(dispatcher)

    strategy = Strategy()

    strategy.set_dispatcher(dispatcher)
    strategy.set_trader(trader)

    datasource_0 = QuoteFeed('trade_feed')
    datasource_0.subscribe(['002230'])

    datasource_1 = QuoteFeed('order_feed')
    datasource_1.subscribe(['002230'])

    strategy.add_datasource(datasource_0)
    strategy.add_datasource(datasource_1)

    strategy.start()

