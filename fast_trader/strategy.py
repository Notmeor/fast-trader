# -*- coding: utf-8 -*-

import datetime, time
import random
import threading
import logging

from fast_trader.dtp_trade import DTP, Trader, Dispatcher
from fast_trader.dtp_quote import (QuoteFeed, conf, Transaction, Snapshot,
                                   MarketOrder, Index, OrderQueue)

from fast_trader.dtp import dtp_api_id as dtp_api_id
from fast_trader.dtp import api_pb2 as dtp_struct
from fast_trader.dtp import type_pb2 as dtp_type

from fast_trader.dtp import ext_api_pb2 as dtp_struct_
from fast_trader.dtp import ext_type_pb2 as dtp_type_

from fast_trader.utils import (timeit, message2dict, load_config, Mail,
                               message2tuple)


config = load_config()


class Market(object):

    def __init__(self):
        self._strategies = []

    def add_strategy(self, strategy):
        self._strategies.append(strategy)

    def on_quote_message(self, message):
        for ea in self._strategies:
            ea.on_quote_message(message)


class Strategy(object):

    def __init__(self, number, *args, **kw):

        self._strategy_id = number

        self._started = False

        self._positions = {}
        self._orders = {}
        self._trades = {}

        self.subscribed_datasources = []

        self.logger = logging.getLogger(
            'fast_trader.strategy.Strategy-{}'.format(number))

    def set_dispatcher(self, dispatcher):
        self.dispatcher = dispatcher

    def set_trader(self, trader):
        self.trader = trader
        self.trader.add_strategy(self)

    def set_market(self, market=None):
        if market is None:
            market = Market()
        self.market = market
        market.add_strategy(self)

    def start(self):

        self.on_start()

        self.trader.start()

        self._account = config['account']
        self.trader.login(
            account=config['account'],
            password=config['password'],
            request_id=self.generate_request_id(),
            sync=True)

        if self.trader.logined:
            self._started = True
            self.on_start()
            self.logger.info('策略启动成功')

            # 启动行情线程
            self.start_market()

        else:
            self.logger.warning(
                '策略启动失败 账户<{}>未成功登录'.format(self.account_no))

    def start_market(self):

        for ds in self.subscribed_datasources:
            ds.start()

    @property
    def strategy_id(self):
        # return '{}_{}'.format(self.account_no, self._strategy_id)
        return self._strategy_id

    @property
    def account_no(self):
        return self.trader._account

    @property
    def position(self):
        return {p['code']: p['balance'] for p in self._positions
                if p.get('balance', 0) != 0}

    def generate_order_id(self):
        return self.trader.generate_order_id(self._strategy_id)

    def generate_request_id(self):
        return self.trader.generate_request_id(self._strategy_id)

    def check_owner(self, obj):
        """
        判断报单查询响应是否属于当前策略
        """
        id_range = self.trader._id_ranges[self.strategy_id]
        return int(obj.order_original_id) in id_range

    @timeit
    def get_positions(self):
        """
        查询持仓（同步）
        """
        request_id = self.generate_request_id()
        mail = self.trader.query_positions(request_id=request_id, sync=True)
        position = self._positions = mail.body.get('position_list', [])
        return position

    def _get_all_pages(self, handle):
        offset = 0
        size = 200
        all_objs = []
        while True:
            request_id = self.generate_request_id()
            mail = handle(request_id=request_id,
                          sync=True,
                          pagination={
                              'size': size,
                              'offset': offset
                          })

            list_name = ''
            for attr in ['order_list', 'fill_list', 'position_list']:
                if hasattr(mail.body, attr):
                    list_name = attr
                    break

            _objs = mail['body'].get(list_name, [])

            all_objs.extend(_objs)
            if len(_objs) < size:
                break
            offset = mail.body.pagination.offset

        objs = [obj for obj in all_objs if self.check_owner(obj)]
        return objs

    @timeit
    def get_orders(self):
        """
        查询报单（同步）
        """
        orders = self._get_all_pages(self.trader.query_orders)
        return orders

    @timeit
    def get_orders_(self):
        """
        查询报单（同步）
        """
        offset = 0
        size = 200
        all_orders = []
        while True:
            request_id = self.generate_request_id()
            mail = self.trader.query_orders(request_id=request_id,
                                            sync=True,
                                            pagination={
                                                'size': size,
                                                'offset': offset
                                            })
            _orders = mail['body'].get('order_list', [])
            print('len:', len(_orders))
            all_orders.extend(_orders)
            if len(_orders) < size:
                break
            offset = mail.body.pagination.offset

        # all_orders = self._orders = mail['body'].get('order_list', {})
        orders = [order for order in all_orders if self.check_owner(order)]
        return orders

    @timeit
    def get_trades(self):
        request_id = self.generate_request_id()
        mail = self.trader.query_trades(request_id=request_id, sync=True)
        trades = self._trades = mail['body'].get('fill_list', [])
        return trades

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
        request_id = self.generate_request_id()
        mail = self.trader.query_capital(request_id=request_id, sync=True)
        return mail['body']

    def get_ration(self):
        """
        查询配售权益
        """
        request_id = self.generate_request_id()
        mail = self.trader.query_ration(request_id=request_id, sync=True)
        return mail['body']

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

        dispatcher = self.dispatcher
        datasource.add_listener(dispatcher)
        try:
            dispatcher.bind('{}_rsp'.format(name),
                            self.market.on_quote_message)

        except Exception as e:
            print(e)
            pass

        self.subscribed_datasources.append(datasource)

        if self._started:
            datasource.start()

    def on_quote_message(self, message):

        api_id = message['api_id']

        if api_id == 'trade_feed':
            data = message2tuple(message['content'], Transaction)
            self.on_market_trade(data)

        elif api_id == 'tick_feed':
            data = message2tuple(message['content'], Snapshot)
            self.on_market_snapshot(data)

        elif api_id == 'order_feed':
            data = message2tuple(message['content'], MarketOrder)
            self.on_market_order(data)

        elif api_id == 'queue_feed':
            data = data = message2tuple(message['content'], OrderQueue)
            self.on_market_queue(data)

        elif api_id == 'index_feed':
            data = message2tuple(message['content'], Index)
            self.on_market_index(data)

    def on_start(self):
        """
        策略启动
        """
        pass

    def on_market_snapshot(self, market_snapshot):
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
        pass

    def on_order_cancelation_submission(self, msg):
        """
        撤单提交响应
        """
        pass

    def on_order_cancelation(self, msg):
        """
        撤单确认回报
        """
        pass

    def on_compliance_report(self, report):
        """
        风控回报
        """
        pass

    def _insert_many(self, order_side, orders):
        request_id = self.generate_request_id()
        for order in orders:
            if 'exchange' not in order:
                order['exchange'] = self.get_exchange(order['code'])
            order['price'] = str(order['price'])
            order['order_side'] = order_side
            order['order_type'] = dtp_type.ORDER_TYPE_LIMIT
            order['order_original_id'] = self.generate_order_id()

        self.trader.place_order_batch(request_id=request_id, orders=orders)
        return orders

    def buy_many(self, orders):
        """
        批量买入
        """
        return self._insert_many(dtp_type.ORDER_SIDE_BUY, orders)

    def sell_many(self, orders):
        """
        批量买入
        """
        return self._insert_many(dtp_type.ORDER_SIDE_SELL, orders)

    def _insert_order(self, **kw):
        request_id = self.generate_request_id()
        order_original_id = self.generate_order_id()
        exchange = kw['exchange'] or self.get_exchange(kw['code'])
        price = str(kw['price'])

        order = kw.copy()
        order.update({
            'request_id': request_id,
            'order_original_id': order_original_id,
            'exchange': exchange,
            'price': price})

        self.trader.send_order(**order)
        return order

    @timeit
    def buy(self, code, price, quantity, exchange=None):
        """
        委托买入

        Parameters
        ----------
        code: str
        price: float
        quantity: int
        exchange: NoneType | int

        Returns
        ----------
        ret: int
            返回报单结构
        """
        return self._insert_order(order_side=dtp_type.ORDER_SIDE_BUY,
                                  code=code, price=price,
                                  quantity=quantity, exchange=exchange)

    def sell(self, code, price, quantity, exchange=None):
        """
        委托卖出

        Parameters
        ----------
        code: str
        price: float
        quantity: int
        exchange: NoneType | int

        Returns
        ----------
        ret: int
            返回报单结构
        """
        return self._insert_order(order_side=dtp_type.ORDER_SIDE_SELL,
                                  code=code, price=price,
                                  quantity=quantity, exchange=exchange)

    def cancel_order(self, **kw):
        """
        撤单

        Parameters
        ----------
        exchange: int
        order_exchange_id: str
        """
        request_id = self.generate_request_id()
        self.trader.cancel_order(request_id=request_id, **kw)

    def cancel_all(self, **kw):
        orders = self.get_open_orders()
        for order in orders:
            self.cancel_order(**order)


# 用于 trader 与 dtp通道 以及 策略实例 间的消息分发
# 将所有行情数据与柜台回报压入同一个队列进行分发，实现策略的同步执行（无需加锁）
dispatcher = Dispatcher()

# dtp通道
dtp = DTP(dispatcher)

# 提供交易接口
trader = Trader(dispatcher, dtp)

market = Market()

def get_strategy_instance(MyStrategyCls, number):
    """
    策略实例化流程演示
    """


    # 策略实例
    strategy = MyStrategyCls(number)

    strategy.set_dispatcher(dispatcher)
    strategy.set_trader(trader)

    strategy.set_market(market)

    return strategy


