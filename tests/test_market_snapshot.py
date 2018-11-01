# -*- coding: utf-8 -*-

import time, datetime
from collections import defaultdict

import numpy as np
import pandas as pd

from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_quote import QuoteFeed
from fast_trader.strategy import Strategy, get_strategy_instance
from fast_trader.utils import timeit, message2dict, int2datetime


def show_title(msg, width=30):
    print(' {} '.format(msg).center(width, '-'))


class MyStrategy(Strategy):

    @property
    def now(self):
        return datetime.datetime.now()

    @property
    def holdings(self):
        positions = self.get_positions()
        return [p['code'] for p in positions]

    def set_params(self, **kw):
        self.params = kw

    def on_start(self):

        self.market_snapshots = defaultdict(list)

        self.on_order_list = []
        self.on_trade_list = []

        # 起始报单时间
        self.ordering_start = datetime.datetime.combine(
                datetime.date.today(), datetime.time(8, 50))

        interval = self.params['ordering_interval']
        self.ordering_interval = datetime.timedelta(seconds=interval)
        self.cur_period = self.ordering_start
        # 报单比率
        self.ordering_ratio = self.params['ordering_ratio']
        # 报单总量
        self.ordering_quota = self.params['ordering_quota']
        # 完成周期
        self.order_range = datetime.timedelta(minutes=1)
        # 总交易量
        self.market_quantity = 0

    def subscribe(self):
        ds = self.subscribed_datasources[0]
        ds.subscribe(self.holdings)

    def clear_holdings(self):

        positions = self.get_positions()

        for pos in positions:

            code = pos['code']
            quantity = pos.get('available_quantity', 0)

            if quantity == 0:
                continue

            try:
                price = self.get_last_price(code)
            except Exception as e:
                self.logger.warning('未取到 {} 最新价格'.format(code))
                continue

            self.sell(code=code, price=price, quantity=quantity)

    def get_last_price(self, code, log=False):

        data = self.market_trades[code][-1]

        if log:
            local_time = int2datetime(data.nActionDay, data.nLocalTime)
            market_time = int2datetime(data.nActionDay, data.nTime)
            self.logger.debug('local_time: {}, market_time: {}'.format(
                abs(local_time - self.now).seconds,
                abs(market_time - self.now).seconds))

        return data.nPrice / 10000

    @timeit
    def get_position_detail_by_code(self, code):
        positions = self.get_positions()
        for p in positions:
            if p['code'] == code:
                return p

    def get_array(self, data, name):
        arr = [getattr(data, '{}_{}'.format(name, i)) for i in range(10)]
        return np.array(arr)

    def on_market_trade(self, market_trade):
        """
        逐笔成交推送
        """
        pass


    def on_market_order(self, market_order):
        """
        逐笔委托推送
        """
        pass

    def on_market_snapshot(self, data):
        """
        行情快照推送
        """

        self.market_snapshots[data.szCode].append(data)

        return
        if data.szCode != '002230':
            return

        '''
        每隔一段时间发出买入委托
        委托价格=卖1到卖10的加权均价
        委托数量=卖1到卖10的总委托量乘以委托系数
        '''
        # nAskVol_0 到 nAskVol_9
        ask_vols = self.get_array(data, 'nAskVol')
        # nAskPrice_0 到 nAskPrice_9
        ask_prices = self.get_array(data, 'nAskPrice') / 10000

        # 十档委托卖出均价
        total_ask_vols = ask_vols.sum()
        avg_price = (ask_vols * ask_prices).sum() / total_ask_vols
        # 委托数量
        quantity = total_ask_vols * self.ordering_ratio

        order_quantity = int(min(quantity, self.ordering_quota) / 100) * 100

        if order_quantity > 0:

            self.buy(data.szCode, round(avg_price, 2), order_quantity)
            self.ordering_quota -= order_quantity
            self.logger.info('分批报单 quantiy={}'.format(order_quantity))

            if self.ordering_quota <= 0:
                self.logger.info('报单全部完成')

    def on_order(self, order):
        show_title('报单回报')
        self.on_order_list.append(order)

    def on_trade(self, trade):
        show_title('成交回报')
        print(trade)
        self.on_trade_list.append(trade)

    def on_order_cancelation(self, msg):
        show_title('撤单回报')
        print(msg)

    def on_order_cancelation_submission(self, msg):
        show_title('报单提交回报')
        print(msg)


if __name__ == '__main__':

    # 策略实例化
    strategy = get_strategy_instance(MyStrategy)

    # 订阅行情
    datasource_0 = QuoteFeed('tick_feed')
    datasource_0.subscribe(['600519'])

    strategy.add_datasource(datasource_0)

    # 设置参数
    strategy.set_params(ordering_interval=10,
                        ordering_ratio=0.01,
                        ordering_quota=10000)

    # 启动策略
    strategy.start()
