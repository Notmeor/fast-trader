# -*- coding: utf-8 -*-

from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_quote import QuoteFeed
from fast_trader.strategy import Strategy, get_strategy_instance
from fast_trader.utils import timeit, message2dict


def show_title(msg, width=30):
    print(' {} '.format(msg).center(width, '-'))


class MyStrategy(Strategy):

    def on_start(self):
        self.market_trades = []
        
    def get_last_price(self, code):
        pass
        
    @timeit
    def get_position_detail_by_code(self, code):
        positions = self.get_position()
        for p in positions:
            if p['code'] == code:
                return p

    def on_market_trade(self, market_trade):
        # print('逐笔成交')
        # data = message2dict(market_trade['content'].body)
        self.market_trades.append(market_trade)

    def on_market_order(self, market_order):
        # print('逐笔委托')
        pass

    def on_order(self, order):
        show_title('报单回报')
        print(order)

    def on_trade(self, trade):
        show_title('成交回报')
        print(trade)

    def on_position_query(self, position):
        print('position:', position)
    
    def on_order_cancelation(self, msg):
        show_title('撤单回报')
        print(msg)
    
    def on_order_cancelation_submission(self, msg):
        show_title('报单提交回报')
        print(msg)


if __name__ == '__main__':

    strategy = get_strategy_instance(MyStrategy)

    datasource_0 = QuoteFeed('trade_feed')
    datasource_0.subscribe(['002230'])

    datasource_1 = QuoteFeed('order_feed')
    datasource_1.subscribe(['002230'])

    strategy.add_datasource(datasource_0)
    strategy.add_datasource(datasource_1)

    strategy.start()
    
    ea = strategy
    
