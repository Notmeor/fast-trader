# -*- coding: utf-8 -*-

import os
os.environ['FAST_TRADER_CONFIG'] = \
    '/Users/eisenheim/Documents/git/fast-trader/tmp/config.yaml'

import time, datetime
from collections import defaultdict

from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_quote import TradeFeed, OrderFeed, TickFeed
from fast_trader.strategy import Strategy, StrategyFactory


class VwapStrategy(Strategy):
    """
    Vwap
    
    每次以盘口卖一的20%买入，直至完成预计买入金额
    """

    strategy_id = 24
    strategy_name = '成交量加权报单'

    def on_start(self):
        """
        执行策略启动前的一些初始化操作
        """

        # 订阅行情
        tk = TickFeed()
        tk.subscribe(['600000', '600519', '002230', '600056'])
        self.add_datasource(tk)
        
        self.available_cash_per_stock = defaultdict(lambda: 100000.)
        self._xx = []

    def on_market_snapshot(self, data):
        """
        响应快照行情
        """

        code = data.szCode
        price = data.nAskPrice_0
        # 卖一量的20%作为当次买入量
        vol = int(data.nAskVol_0 * 0.2 / 100) * 100
        
        if vol > 0 and self.available_cash_per_stock[code] > 0:
            self.buy(code, price, vol)
            order_amount = price * vol
            self.available_cash_per_stock[code] -= order_amount
            self.logger.info(f'{code} 买入 {order_amount}, '
                             f'剩余可买 {self.available_cash_per_stock[code]}')

    def on_order(self, order):
        """
        响应报单回报
        """
        print('\n-----报单回报-----')
        print(order)

    def on_trade(self, trade):
        """
        响应成交回报
        """
        print('\n-----成交回报-----')
        print(trade)

    def on_order_cancelation(self, data):
        """
        响应撤单回报
        """
        print('\n-----撤单回报-----')
        print(data)


if __name__ == '__main__':


    factory = StrategyFactory()
    strategy = factory.generate_strategy(
        VwapStrategy,
        strategy_id=VwapStrategy.strategy_id
    )

    strategy.start()
    
    ea = strategy
    acc = ea._ledger_writer.accountant

#    strategy_1 = factory.generate_strategy(
#        VwapStrategy,
#        strategy_id=VwapStrategy.strategy_id + 1
#    )
#
#    strategy_1.start()

