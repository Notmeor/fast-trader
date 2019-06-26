
import os
os.environ['FAST_TRADER_CONFIG'] = \
    '/Users/eisenheim/Documents/git/fast-trader/tmp/config.yaml'

import time
import threading
import unittest

from fast_trader.dtp_trade import dtp_api, DTP, Dispatcher, dtp_type, Trader


class TestTrader:

    def __init__(self):
        self.dispatcher = Dispatcher()
        self.trade_api = DTP(self.dispatcher)
        self.trader = Trader(
            dispatcher=self.dispatcher,
            trade_api=self.trade_api,
            trader_id=0)

    def test_get_capital(self):
        capital = self.trader.query_capital()
        print('Capital: ', capital)

    def test_get_orders(self):
        orders = self.trader.query_orders()
        print('order count: ', len(orders))

    def test_get_trades(self):
        trades = self.trader.query_trades()
        print('trade count: ', len(trades))

    def test_get_positions(self):
        positions = self.trader.query_positions()
        print('pos count: ', len(positions))
    
    def run_all(self):
        self.test_get_capital()
        self.test_get_orders()
        self.test_get_trades()
        self.test_get_positions()

if __name__ == '__main__':

    test = TestTrader()

    test.run_all()