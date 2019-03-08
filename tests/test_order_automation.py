# -*- coding: utf-8 -*-

import time, datetime
from collections import defaultdict

from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_quote import TradeFeed, OrderFeed
from fast_trader.strategy import Strategy, StrategyFactory
from fast_trader.utils import timeit, message2dict, int2datetime


def get_target_holding_amount():
    return {
        '002230.SZ': 10000.,
        '601555.SH': 10000.,
    }


class MyStrategy(Strategy):

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

        # 目标持仓金额
        self.target_holding_amount = get_target_holding_amount()

        self.target_codes = list(self.target_holding_amount.keys())

        # 委托单占用的现金
        self.pending_amount = defaultdict(lambda: 0.)
        # 待成交金额（不包含委托占用资金）
        self.unrealized_amount = self.target_holding_amount.copy()

        # 设置滑点
        self.param_slipages = 1

    @timeit
    def get_position_detail_by_code(self, code):
        positions = self.get_positions()
        for p in positions:
            if p['code'] == code:
                return p

    def on_market_trade(self, data):
        """
        响应逐笔成交行情
        """

        if data.nTime < 93000000:  # 不参与集合竞价
            return

        code = data.szWindCode
        if code in self.target_codes:
            left_amount = self.unrealized_amount[code]
            price = data.nPrice + self.param_slippage * 0.01
            volume = int(left_amount / price / 100) * 100
            if volume > 0:
                self.buy(data.code, price, volume)
                self.unrealized_amount[code] -= price * volume

                self.logger.warning(f'{code} 委托买入 {volume} 股')

    def on_market_order(self, data):
        """
        响应逐笔报单行情(上交所无该数据推送)
        """
        pass

    def on_market_snapshot(self, data):
        """
        响应快照行情
        """
        pass

    def on_order(self, order):
        """
        响应报单回报
        """
        pass

    def on_trade(self, trade):
        """
        响应成交回报
        """
        pass

    def on_order_cancelation(self, msg):
        """
        响应撤单回报
        """
        print(msg)



if __name__ == '__main__':

    from fast_trader.settings import settings
    from fast_trader.utils import get_mac_address
    settings.set({
        'ip': '192.168.211.169',
        'mac': get_mac_address(),
        'harddisk': '6B69DD46',
    })

    factory = StrategyFactory()
    strategy = factory.generate_strategy(
        MyStrategy, 
        trader_id=1, 
        strategy_id=1
    )

    tf = TradeFeed()
    tf.subscribe(['300104', '002230', '000001'])

    of = OrderFeed()
    of.subscribe(['300104', '002230'])

    strategy.add_datasource(tf)
    strategy.add_datasource(of)

    strategy.start()

    ea = strategy




