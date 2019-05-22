# -*- coding: utf-8 -*-

import os
os.environ['FAST_TRADER_CONFIG'] = \
    '/Users/eisenheim/Documents/git/fast-trader/tmp/config.yaml'

import time, datetime
from collections import defaultdict

from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_quote import TradeFeed, OrderFeed, TickFeed
from fast_trader.strategy import Strategy, StrategyFactory, to_timeint
from fast_trader.utils import timeit, message2dict, int2datetime, attrdict


class Batch50EtfStrategy(Strategy):
    """
    Demo
    """

    strategy_id = 14
    strategy_name = '一篮子买入上证50成分股'

    @property
    def now(self):
        return datetime.datetime.now()

    def on_start(self):
        """
        执行策略启动时的一些初始化操作
        """
        self.set_initial_capital(1000000.)

        self.total_amount_to_buy = 1000000.
        self.total_fill_amount = 0.

        sz50 = [
            '601318.SH', '600519.SH', '600036.SH', '601166.SH', '600887.SH',
            '601328.SH', '600276.SH', '600030.SH', '600016.SH', '601288.SH',
            '600000.SH', '601398.SH', '601668.SH', '601601.SH', '600048.SH',
            '601169.SH', '600104.SH', '601988.SH', '601766.SH', '600585.SH',
            '601888.SH', '601211.SH', '600028.SH', '601229.SH', '601818.SH',
            '600309.SH', '600019.SH', '601688.SH', '601857.SH', '600690.SH',
            '600050.SH', '600340.SH', '601390.SH', '601006.SH', '601939.SH',
            '601989.SH', '601186.SH', '601628.SH', '601336.SH', '601088.SH',
            '603993.SH', '601800.SH', '600196.SH', '600606.SH', '600029.SH',
            '600547.SH', '600703.SH', '601138.SH', '601360.SH', '603259.SH'
        ]
        self.universe = [s.split('.')[0] for s in sz50]

        # 订阅行情
        tk = TickFeed()
        tk.subscribe(self.universe)

        td = TradeFeed()
        td.subscribe(self.universe)

        self.add_datasource(tk)
        self.add_datasource(td)
        print('订阅')

        self.buy_order_placed = defaultdict(lambda: False)

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


        code = data.szCode
        if not self.buy_order_placed[code] and data.nMatch > 0:
            price = data.nMatch + 0.01
            quantity = int(self.total_amount_to_buy
                           / len(self.universe) / price / 100) * 100
            if quantity > 0:
                self.buy(code, price, quantity)
                self.buy_order_placed[code] = True


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

        self.total_fill_amount += trade.fill_amount
        ratio = self.total_fill_amount / self.total_amount_to_buy * 100
        self.logger.info(f'订单买入完成度：{ratio: .2f}%')

    def on_order_cancelation(self, data):
        """
        响应撤单回报
        """
        print('\n-----撤单回报-----')
        print(data)


if __name__ == '__main__':


    factory = StrategyFactory()
    strategy = factory.generate_strategy(
        Batch50EtfStrategy,
        strategy_id=Batch50EtfStrategy.strategy_id,
        account_no='011000106328',
    )

    strategy.start()


