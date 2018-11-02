# -*- coding: utf-8 -*-

import time, datetime
from collections import defaultdict

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
        """
        约定参数均以 `p_` 开头，避免与其它变量冲突
        """
        self.params = kw

    def on_start(self):
        
        self.market_trades = defaultdict(list)
        self.market_snapshots = defaultdict(list)

        self.on_order_list = []
        self.on_trade_list = []

        # 起始报单时间
        self.ordering_start = datetime.datetime.combine(
                datetime.date.today(), datetime.time(8, 50))
        self.ordering_interval = datetime.timedelta(seconds=5)
        self.cur_period = self.ordering_start
        # 报单比率
        self.ordering_ratio = 0.01
        # 报单总量
        self.ordering_quota = 10000
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

    def on_market_trade(self, data):

        if data.nPrice > 0:
            self.market_trades[data.szCode].append(data)

        return 

        if not data.szCode == '002230':
            return
        
        if len(self.market_trades[data.szCode]) < 1:
            return

        self.cur_period = int2datetime(n_date=data.nActionDay,
                                       n_time=data.nTime)
        self.market_quantity += data.nVolume

        if self.cur_period - self.ordering_start >= self.ordering_interval:
            price = self.get_last_price(data.szCode)
            units = int(self.market_quantity * self.ordering_ratio / 100)

            self.ordering_start = self.cur_period
            self.market_quantiy = 0

            quantity = min(units * 100, self.ordering_quota)
            if quantity > 0:
                self.buy(data.szCode, price, quantity)
                self.ordering_quota -= quantity
                self.logger.info('分批报单 quantiy={}'.format(quantity))

                if self.ordering_quota <= 0:
                    self.logger.warning('报单全部完成')

    def on_market_order(self, market_order):
        pass

    def on_market_snapshot(self, data):
        self.market_snapshots[data.szCode].append(data)

    def on_order(self, order):
        # show_title('报单回报')
        self.on_order_list.append(order)
        self.logger.info(order)

    def on_trade(self, trade):
        # show_title('成交回报')
        print(trade)
        self.on_trade_list.append(trade)
        self.logger.info(trade)

    def on_order_cancelation(self, msg):
        show_title('撤单回报')
        print(msg)

    def on_order_cancelation_submission(self, msg):
        show_title('撤单提交回报')
        print(msg)


if __name__ == '__main__':

    import logging
    logger = logging.getLogger('test')
    
    from fast_trader.dtp_trade import DTP, Trader, Dispatcher
    
    dispatcher = Dispatcher()

    # dtp通道
    dtp = DTP(dispatcher)

    # 提供交易接口
    trader = Trader(dispatcher, dtp)

    # 策略实例
    strategy_2 = MyStrategy(1)


    strategy_2.set_dispatcher(dispatcher)
    strategy_2.set_trader(trader)




#    datasource_0 = QuoteFeed('trade_feed')
#    datasource_0.subscribe(['300104', '002230', '000001'])
#
#    datasource_1 = QuoteFeed('tick_feed')
#    datasource_1.subscribe(['300104', '002230'])
#
#    strategy_2.add_datasource(datasource_0)
#    strategy_2.add_datasource(datasource_1)
#    

#    strategy_5.add_datasource(datasource_0)
#    strategy_5.add_datasource(datasource_1)
    

    strategy_2.start()
    
    strategy_2.buy('002230', 22, 1000)
    logger.info("strategy_2.buy('002230', 22, 1000)")

#    strategy_5 = MyStrategy(5)
#    strategy_5.set_dispatcher(dispatcher)
#    strategy_5.set_trader(trader)
#    strategy_5.start()
#    
#    strategy_5.buy('002230', 21, 1000)
#    logger.info("strategy_5.buy('002230', 21, 1000)")




