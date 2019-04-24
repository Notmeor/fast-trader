# -*- coding: utf-8 -*-

import time, datetime
from collections import defaultdict

from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_quote import TradeFeed, OrderFeed, TickFeed
from fast_trader.strategy import Strategy, StrategyFactory, to_timeint
from fast_trader.utils import timeit, message2dict, int2datetime, attrdict



class DemoStrategy(Strategy):
    """
    Demo
    """
    
    strategy_id = 8
    strategy_name = 'Demo strategy'

    @property
    def now(self):
        return datetime.datetime.now()

    @property
    def holdings(self):
        positions = self.get_positions()
        return [p['code'] for p in positions]

    def on_start(self):
        """
        执行策略启动前的一些初始化操作
        """
        pass

    def on_market_trade(self, data):
        """
        响应逐笔成交行情
        """

        # 过滤撤单记录
        if data.nTurnover == 0:
            return

        if data.nTime < 93000000:  # 不参与集合竞价
            return

    def on_market_snapshot(self, data):
        """
        响应快照行情
        """
        pass

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
        DemoStrategy,
        trader_id=1,
        strategy_id=DemoStrategy.strategy_id
    )
    
    # 订阅行情
    tf = TradeFeed()
    tf.subscribe(['600056'])

    of = OrderFeed()
    of.subscribe(['600056'])

    tk = TickFeed()
    tk.subscribe(['600056', '601668', '601555'])

    strategy.add_datasource(tf)
    strategy.add_datasource(tk)

    strategy.start()

    ## 下单
    # strategy.buy('601555', '16', 500)
    ## 全撤
    # strategy.cancle_all()

