import time, datetime

from fast_trader.dtp_trade import dtp_type
from fast_trader.dtp_quote import TradeFeed, OrderFeed, TickFeed
from fast_trader.strategy import Strategy, StrategyFactory, to_timeint
from fast_trader.utils import timeit, int2datetime, attrdict


class DemoStrategy(Strategy):
    """
    测试策略撤单
    """

    strategy_id = 3
    strategy_name = '部分成交撤单demo'

    def on_start(self):
        """
        响应策略启动
        """

        #self.subscribe(TickFeed, ['600052', '603629', '002230'])
        self.last_order = self.buy('002230', 14, 100)

    def on_market_snapshot(self, data):
        print(data.szCode, data.nMatch)
    
    def on_market_trade(self, data):
        print('-----逐笔成交-----')
        print(data.nTime, data.szWindCode, data.nPrice)

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

        if self.last_order.status == dtp_type.ORDER_STATUS_PARTIAL_FILLED:
            self.cancel_order(**self.last_order)

    def on_order_cancelation(self, data):
        """
        响应撤单回报
        """
        print('\n-----撤单回报-----')
        print(data)

