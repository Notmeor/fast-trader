# -*- coding: utf-8 -*-

import os

import time, datetime
from collections import defaultdict
import random

from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_quote import TradeFeed, OrderFeed, TickFeed
from fast_trader.strategy import Strategy, StrategyFactory, to_timeint
from fast_trader.utils import timeit, message2dict, int2datetime, attrdict


class DemoStrategy(Strategy):
    """
    Demo
    """

    strategy_id = 23
    strategy_name = '报单测试'

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

        self.placed_orders = {}
        self.universe = [
            '601318', '600519', '600036', '601166', '600887',
            '601328', '600276', '600030', '600016', '601288',
        ]

        for code in self.universe:
            price = random.randint(5, 30)
            order = self.buy(code, price, 2000)
            self.placed_orders[code] = order

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
        
        order = self.placed_orders.get(trade.order_original_id, None)
        if order:
            if order.status == dtp_type.ORDER_STATUS_PARTIAL_FILLED:
                self.cancel_order(**order)

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
        strategy_id=DemoStrategy.strategy_id,
        account_no='011000106328',
    )

    strategy.start()
