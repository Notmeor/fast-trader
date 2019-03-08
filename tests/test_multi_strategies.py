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

from fast_trader.position_store import SqlitePositionStore as PositionStore


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

        self._orders = {}

        self.date_str = datetime.date.today().strftime('%Y%m%d')
        self.position_store = PositionStore()
        self.order_all_filled = True
        self.max_holding_quantity = 100000

        self.allow_trading = False
        self.bought_quantity = 0

        self.market_trades = defaultdict(list)
        self.market_snapshots = defaultdict(list)
        self.market_queues = defaultdict(list)

        self.on_order_list = []
        self.on_trade_list = []

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
        positions = self.get_account_positions()
        for p in positions:
            if p['code'] == code:
                return p

    def on_market_trade(self, data):

        if data.nPrice > 0:
            self.market_trades[data.szCode].append(data)

        if not data.szCode == '002230':
            return

        time.sleep(0.2)
        if len(self.market_trades[data.szCode]) < 100:
            return

        if not self.allow_trading:
            return

        ds = self.market_trades[data.szCode]
        mv_0 = np.mean([d.nPrice for d in ds[-50:]])

        if ds[-1].nPrice > mv_0:

            # cur_pos = self.get_position_by_code(data.szCode)
            cur_pos = self.positions[data.szCode]

            if not self.order_all_filled:
                return

            if cur_pos['quantity'] < self.max_holding_quantity:
            # if self.bought_quantity < 30000:
                self.buy(data.szCode,
                         round(ds[-1].nPrice / 10000 + 0.01, 2),
                         10000)
                self.logger.info('买入')
                self.bought_quantity += 10000

                self.order_all_filled = False

#        if ds[-1].nPrice < mv_0:
#            if self.bought_quantity > 0:
#                self.sell(data.szCode,
#                          round(ds[-1].nPrice / 10000 - 0.01, 2),
#                          10000)
#                self.logger.info('卖出')
#                self.bought_quantity -= 10000

    def on_market_queue(self, data):
        self.market_queues[data.szCode].append(data)

    def on_market_order(self, market_order):
        pass

    def on_market_snapshot(self, data):
        self.market_snapshots[data.szCode].append(data)

    def on_order_tmp(self, order):
        # show_title('报单回报')
        self.on_order_list.append(order)
        self.logger.info(order)

    def on_trade_tmp(self, trade):
        show_title('成交回报')

        if trade.fill_status == dtp_type.FILL_STATUS_FILLED:
            self.order_all_filled = True

        self.on_trade_list.append(trade)
        self.logger.info(trade)

    def on_order_cancelation(self, msg):
        show_title('撤单回报')
        print(msg)

    def on_order_cancelation_submission(self, msg):
        show_title('撤单提交回报')
        print(msg)

def test_mq():
    self = ea.dispatcher
    try:
        # mail = self._outbox.get(timeout=0.000001)
        mail = self._outbox.get(block=False)
        self.dispatch(mail)
    except queue.Empty:
        pass

    try:
        mail = self._market_queue.get(block=False)
        self.dispatch(mail)
    except queue.Empty:
        pass

def test(ea, amount):
    self = ea
    code = '002230'
    cur_pos = self.position_store.get_position_by_code(
        self.strategy_id, code)
    print(cur_pos)

    if cur_pos['quantity'] < amount:

        self.buy(code,
                 round(22, 2),
                 2000)
        self.logger.info('买入')
    else:
        raise Exception('stop')

def test_limit(interval=0.1, amount=800000):
    while True:
        time.sleep(interval)
        test(strategy_6, amount)

if __name__ == '__main__':
    
    from data_provider.datafeed.universe import Universe
    sz50 = Universe().get_index_compose_weight_by_date('000016.SH', '20181109')
    codes = sz50.securityId.tolist()
    codes = [c[:6] for c in codes]

    import logging
    logger = logging.getLogger('test')

    datasource_0 = QuoteFeed('trade_feed')
    # datasource_0.subscribe(['601607', '600519', '300104', '002230', '000001'])
    datasource_0.subscribe(codes)

    datasource_1 = QuoteFeed('tick_feed')
    datasource_1.subscribe(codes)

    datasource_2 = QuoteFeed('queue_feed')
    datasource_2.subscribe(codes)

    # 策略实例
    strategy_6 = get_strategy_instance(MyStrategy, 8)
    strategy_7 = get_strategy_instance(MyStrategy, 9)

    strategy_6.add_datasource(datasource_0)
    strategy_6.add_datasource(datasource_1)
    strategy_6.add_datasource(datasource_2)

    strategy_7.add_datasource(datasource_0)
    strategy_7.add_datasource(datasource_1)
    strategy_7.add_datasource(datasource_2)

    strategy_6.start()
    strategy_7.start()

    # strategy_2.cancel_all()
