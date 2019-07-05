import os
import threading
import time, datetime
from collections import defaultdict

from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_quote import TradeFeed, OrderFeed, TickFeed
from fast_trader.strategy import Strategy, StrategyFactory, to_timeint
from fast_trader.utils import timeit, message2dict, int2datetime, attrdict


class DemoStrategy(Strategy):
    """
    测试策略撤单
    """

    strategy_id = 40
    strategy_name = '测试策略撤单'

    def on_start(self):
        """
        响应策略启动
        """
        
        print(datetime.datetime.now(), f'启动响应:{(os.getpid(), threading.get_ident())}')
        self.subscribe(TradeFeed, ['600052', '603629'])
        self.subscribe_all(TradeFeed)
        self.last_order = self.buy('002230', 14, 100)
        
        def f():
            print(datetime.datetime.now())
        
        #self.run_at_intervals(interval=datetime.timedelta(seconds=2), func=f)

    def on_market_snapshot(self, data):
        print(data)
    
    def on_market_trade(self, data):
        print(f'\n-----逐笔成交-----{(os.getpid(), threading.get_ident())}')
        print(data.nTime, data.szWindCode, data.nPrice)

    def on_order(self, order):
        """
        响应报单回报
        """
        print(f'\n-----报单回报-----{(os.getpid(), threading.get_ident())}')
        print(order)

    def on_trade(self, trade):
        """
        响应成交回报
        """
        print(f'\n-----成交回报-----{(os.getpid(), threading.get_ident())}')
        print(trade)

        if self.last_order.status == dtp_type.ORDER_STATUS_PARTIAL_FILLED:
            self.cancel_order(**self.last_order)

    def on_order_cancelation(self, data):
        """
        响应撤单回报
        """
        print('\n-----撤单回报-----')
        print(data)


if __name__ == '__main__':

    factory = StrategyFactory()
    strategy = factory.generate_strategy(
        DemoStrategy,
        strategy_id=DemoStrategy.strategy_id + 1,
        account_no='011000106328',
    )

    strategy.start()
    ea = strategy
