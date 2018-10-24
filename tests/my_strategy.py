# -*- coding: utf-8 -*-

from fast_trader.strategy import Strategy


class MyStrategy(Strategy):
    
    def on_market_trade(self, trade):
        pass
    
    def on_order(self, order):
        pass
    
    def on_trade(self, trade):
        pass