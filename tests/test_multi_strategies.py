
import os
os.chdir('D:/work/PB_newUAT/PB客户端_新UAT_自由委托/策略交易/')
import threading
import time, datetime
from collections import defaultdict

from fast_trader.dtp_trade import dtp_type
from fast_trader.dtp_quote import TradeFeed, OrderFeed, TickFeed
from fast_trader.strategy import Strategy, StrategyFactory, to_timeint
from fast_trader.utils import timeit, int2datetime


class DemoStrategy(Strategy):
    """
    测试策略撤单
    """

    strategy_id = 50
    strategy_name = '测试策略撤单'

    def on_start(self):
        """
        响应策略启动
        """
        self.subscribe(TickFeed, ['002230', '600890', '600052', '603629'])
        self.last_order = self.buy('002230', 14, 100)

    def on_market_snapshot(self, data):
        #print(data.szCode, data.nMatch)
        pass

    def on_market_trade(self, data):
        print(f'\r{data.szCode, data.nPrice, os.getpid(), threading.get_ident()}',
              end='')

    def on_order(self, order):
        """
        响应报单回报
        """
        print('\n-----报单回报-----', os.getpid(), threading.get_ident())
        print(order)

    def on_trade(self, trade):
        """
        响应成交回报
        """
        print('\n-----成交回报-----')
        print(trade)

        if self.last_order.status == dtp_type.ORDER_STATUS_PARTIAL_FILLED:
            self.cancel_order(**self.last_order)

    def on_order_cancelation(self, data):
        """
        响应撤单回报
        """
        print('\n-----撤单回报-----', os.getpid(), threading.get_ident())
        print(data)


if __name__ == '__main__':

    factory = StrategyFactory()
    strategy = factory.generate_strategy(
        DemoStrategy,
        strategy_id=DemoStrategy.strategy_id,
        account_no='011000106328',
    )

    strategy_1 = factory.generate_strategy(
        DemoStrategy,
        strategy_id=DemoStrategy.strategy_id + 1,
        account_no='011000106328',
    )
    
    strategy.start()
    strategy_1.start()
