# -*- coding: utf-8 -*-

import datetime, time
import random
import threading
import logging

from fast_trader.dtp_trade import DTP, Trader, Dispatcher
from fast_trader.dtp_quote import QuoteFeed, conf

from fast_trader.dtp import dtp_api_id as dtp_api_id
from fast_trader.dtp import api_pb2 as dtp_struct
from fast_trader.dtp import type_pb2 as dtp_type

from fast_trader.dtp import ext_api_pb2 as dtp_struct_
from fast_trader.dtp import ext_type_pb2 as dtp_type_

from fast_trader.utils import timeit, message2dict, load_config, Mail


config = load_config()


def generate_request_id():
    return str(random.randrange(11000000, 11900000))


def generate_original_id():
    return str(random.randrange(61000000, 61900000))


class Strategy(object):

    def __init__(self, *args, **kw):

        self._started = False

        self._positions = {}
        self._orders = {}
        self._trades = {}

        self.subscribed_datasources = []

        self.logger = logging.getLogger('fast_trader.strategy')

    def set_dispatcher(self, dispatcher):
        self.dispatcher = Dispatcher()

    def set_trader(self, trader):
        self.trader = trader

    def start(self):

        self.on_start()

        # 启动行情线程
        self.start_market()

        self.trader.add_strategy(self)
        self.trader.start()

        self._account = config['account']
        self.trader.login(
            account=config['account'],
            password=config['password'],
            sync=True)

        if self.trader.logined:
            self._started = True
            self.logger.info('策略启动成功')
        else:
            self.logger.info(
                '策略启动失败 账户<{}>未成功登录'.format(self.account_no))

    def start_market(self):

        for ds in self.subscribed_datasources:
            ds.start()

    @property
    def account_no(self):
        return self.trader._account

    @property
    def position(self):
        return {p['code']: p['balance'] for p in self._positions
                if p.get('balance', 0) != 0}

    @timeit
    def get_positions(self):
        """
        查询持仓（同步）
        """
        mail = self.trader.query_positions(sync=True)
        payload = mail['content']
        msg = message2dict(payload.body)
        position = self._positions = msg['position_list']
        return position

    @timeit
    def get_orders(self):
        """
        查询报单（同步）
        """
        mail = self.trader.query_orders(sync=True)
        payload = mail['content']
        msg = message2dict(payload.body)
        orders = self._orders = msg.get('order_list', {})
        return orders

    @timeit
    def get_trades(self):
        mail = self.trader.query_orders(sync=True)
        payload = mail['content']
        msg = message2dict(payload.body)
        orders = self._orders = msg.get('order_list', {})
        return orders
    
    @timeit
    def get_open_orders(self):
        """
        查询未成交报单（同步）

        Note
        --------
        enum OrderStatus
        {
            ORDER_STATUS_UNDEFINED = 0;
            ORDER_STATUS_PLACING = 1;               // 正报: 交易所处理中
                                                    // (order_exchange_id已产生)
            ORDER_STATUS_PLACED = 2;                // 已报: 交易所已挂单
            ORDER_STATUS_PARTIAL_FILLED = 3;        // 部分成交
            ORDER_STATUS_FILLED = 4;                // 全部成交
            ORDER_STATUS_CANCELLING = 5;            // 待撤
            ORDER_STATUS_CANCELLED = 6;             // 已撤
            ORDER_STATUS_PARTIAL_CANCELLING = 7;    // 部分成交其余待撤
            ORDER_STATUS_PARTIAL_CANCELLED = 8;     // 部分成交其余已撤
            ORDER_STATUS_FAILED = 9;                // 废单
        }
        """
        orders = self.get_orders()
        open_orders = [order for order in orders if order['status'] < 4]
        return open_orders

    @timeit
    def get_capital(self):
        """
        查询资金（同步）
        """
        mail = self.trader.query_capital(sync=True)
        payload = mail['content']
        return message2dict(payload.body)

    def get_exchange(self, code):
        """
        返回交易所代码
        """
        if code.startswith('6'):
            return dtp_type.EXCHANGE_SH_A
        else:
            return dtp_type.EXCHANGE_SZ_A

    def add_datasource(self, datasource):
        """
        添加行情数据源

        Parameters
        ----------
        datasource: QuoteFeed
            可选的数据源包括：
                trade_feed # 逐笔成交
                order_feed # 逐笔报单
                tick_feed  # 快照行情
                queue_feed # 委托队列
                index_feed # 指数行情
        """

        name = datasource.name

        handler = {
           'trade_feed': self.on_market_trade,
           'tick_feed': self.on_tick,
           'queue_feed': self.on_market_queue,
           'order_feed': self.on_market_order,
           'index_feed': self.on_market_index
        }[name]

        dispatcher = self.dispatcher
        datasource.add_listener(dispatcher)
        dispatcher.bind('{}_rsp'.format(name), handler)

        self.subscribed_datasources.append(datasource)

        if self._started:
            datasource.start()

    def on_start(self):
        """
        策略启动
        """
        pass

    def on_market_trade(self, market_trade):
        """
        逐笔成交行情
        """
        pass

    def on_tick(self, tick):
        """
        快照行情
        """
        pass

    def on_market_queue(self, market_queue):
        """
        委托队列行情
        """
        pass

    def on_market_order(self, market_order):
        """
        逐笔报单行情
        """
        pass

    def on_market_index(self, market_index):
        """
        指数行情
        """
        pass

    def on_trade(self, trade):
        """
        成交回报
        """
        pass

    def on_order(self, order):
        """
        订单回报
        """
        pass

    def on_batch_order_submission(self, msg):
        """
        批量委托响应
        """
        pass

    def on_order_query(self, orders):
        """
        报单查询
        """

    def on_order_cancelation_submission(self, msg):
        """
        撤单提交响应
        """

    def on_order_cancelation(self, msg):
        """
        撤单确认回报
        """

    def on_trade_query(self, trades):
        """
        成交查询回报
        """
        pass

    def on_position_query(self, position):
        """
        持仓查询回报
        """
        pass

    def on_capital_query(self, account):
        """
        资金查询回报
        """
        pass

    def on_compliance_report(self, report):
        """
        风控回报
        """
        pass

    def buy_many(self, orders):
        """
        批量买入
        """
        for order in orders:
            order['exchange'] = self.get_exchange(order['code'])
            order['price'] = str(order['price'])
            order['order_side'] = dtp_type.ORDER_SIDE_BUY
            order['order_type'] = dtp_type.ORDER_TYPE_LIMIT
            order['order_original_id'] = generate_original_id()

        self.trader.place_order_batch(orders)

    def sell_many(self, orders):
        """
        批量买入
        """
        for order in orders:
            order['exchange'] = self.get_exchange(order['code'])
            order['price'] = str(order['price'])
            order['order_side'] = dtp_type.ORDER_SIDE_SELL
            order['order_type'] = dtp_type.ORDER_TYPE_LIMIT

        self.trader.place_order_batch(orders)

    def buy(self, code, price, quantity):
        """
        委托买入

        Parameters
        ----------
        code: str
        price: float
        quantity: int
        """
        order_original_id = generate_original_id()
        exchange = self.get_exchange(code)
        price = str(price)
        self.trader.send_order(
            order_original_id=order_original_id,
            exchange=exchange, code=code,
            price=price, quantity=quantity,
            order_side=dtp_type.ORDER_SIDE_BUY)

    def sell(self, code, price, quantity):
        """
        委托卖出

        Parameters
        ----------
        code: str
        price: float
        quantity: int
        """
        order_original_id = generate_original_id()
        exchange = self.get_exchange(code)
        price = str(price)
        self.trader.send_order(
            order_original_id=order_original_id,
            exchange=exchange, code=code,
            price=price, quantity=quantity,
            order_side=dtp_type.ORDER_SIDE_SELL)

    def cancel_order(self, **kw):
        """
        撤单

        Parameters
        ----------
        exchange: int
        order_exchange_id: str
        """
        exchange = kw['exchange']
        order_exchange_id = kw['order_exchange_id']
        self.trader.cancel_order(**kw)


def get_strategy_instance(MyStrategyCls):
    """
    策略实例化流程演示
    """

    # 用于 trader 与 dtp通道 以及 策略实例 间的消息分发
    # 将所有行情数据与柜台回报压入同一个队列进行分发，实现策略的同步执行（无需加锁）
    dispatcher = Dispatcher()

    # dtp通道
    dtp = DTP(dispatcher)

    # 提供交易接口
    trader = Trader(dispatcher, dtp)

    # 策略实例
    strategy = MyStrategyCls()

    strategy.set_dispatcher(dispatcher)
    strategy.set_trader(trader)

    return strategy


if __name__ == '__main__':

    strategy = get_strategy_instance(Strategy)

    datasource_0 = QuoteFeed('trade_feed')
    datasource_0.subscribe(['002230'])

    datasource_1 = QuoteFeed('order_feed')
    datasource_1.subscribe(['002230'])

    strategy.add_datasource(datasource_0)
    strategy.add_datasource(datasource_1)

    strategy.start()

