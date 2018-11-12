# -*- coding: utf-8 -*-

import logging
import collections

from fast_trader.dtp_trade import DTP, Trader, Dispatcher
from fast_trader.dtp_quote import (Transaction, Snapshot,
                                   MarketOrder, Index, OrderQueue)

from fast_trader.dtp import type_pb2 as dtp_type

from fast_trader.position_store import SqlitePositionStore as PositionStore
from fast_trader.utils import (timeit, load_config, message2tuple)


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
        self._orders = collections.defaultdict(dict)
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

    def set_position_store(self, store=None):
        if store is None:
            store = PositionStore()
        self._position_store = store

    def start(self):

        self.on_start()

        # xx
        self.set_position_store()

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
        判断该查询响应数据是否属于当前策略
        """
        id_range = self.trader._id_ranges[self.strategy_id]
        return int(obj.order_original_id) in id_range

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

        return all_objs

    @timeit
    def get_orders(self):
        """
        查询报单（同步）
        """
        orders = self._get_all_pages(self.trader.query_orders)
        orders = [order for order in orders if self.check_owner(order)]
        return orders

    @timeit
    def get_trades(self):
        """
        查询成交（同步）
        """
        trades = self._get_all_pages(self.trader.query_trades)
        trades = [trade for trade in trades if self.check_owner(trade)]
        return trades

    @timeit
    def get_account_positions(self):
        """
        查询账户总持仓（同步）
        """
        positions = self._get_all_pages(self.trader.query_positions)
        return positions

    def get_positions(self):
        """
        查询策略持仓

        从PositionStore中读取数据
        """
        return self._position_store.get_positions(self.strategy_id)

    def get_position_by_code(self, code, exchange=None):
        return self._position_store.get_position_by_code(
                strategy_id=self.strategy_id,
                code=code,
                exchange=exchange)

    def update_position(self, trade):

        code = trade.code
        order_side = trade.order_side
        fill_quantity = trade.fill_quantity
        last_pos = self.get_position_by_code(code)

        last_quantity = last_pos['quantity']
        last_cost_price = last_pos['cost_price'] or 0.

        _sign = 1 if order_side == dtp_type.ORDER_SIDE_BUY else -1

        quantity = fill_quantity * _sign + last_quantity
        if order_side == dtp_type.ORDER_SIDE_BUY:
            tot_value = (last_quantity * last_cost_price +
                         fill_quantity * float(trade.price))
            tot_quantity = (last_quantity + trade.fill_quantity)
            cost_price = tot_value / tot_quantity
        else:
            cost_price = last_cost_price

        self._position_store.set_positions([
            {
                'strategy_id': self.strategy_id,
                'exchange': trade.exchange,
                'code': trade.code,
                'quantity': quantity,
                'cost_price': cost_price,
                'date': self.date_str,
                'time': trade.fill_time
            }
        ])

    @property
    def positions(self):
        if not hasattr(self, '_position_query_proxy'):
            class _Positions(object):
                def __getitem__(self_, code):
                    return self.get_position_by_code(code)
            self._position_query_proxy = _Positions()
        return self._position_query_proxy

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

        # FIXME: bind once only
        try:
            dispatcher.bind('{}_rsp'.format(name),
                            self.market.on_quote_message)
            
            datasource.add_listener(dispatcher)

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

    def on_trade(self, msg):
        """
        成交回报
        """
        trade = msg.body
        if msg.header.code == dtp_type.RESPONSE_CODE_OK:
            original_id = trade.order_original_id
            order_detail = self._orders[original_id]
            order_detail.update(trade)

        if msg.body.fill_status != 1:
            self.logger.error(msg)
        else:
            # 更新本地持仓记录
            self.update_position(trade)

            self.on_trade_tmp(trade)

    def on_order(self, msg):
        """
        订单回报
        """
        order = msg.body
        if msg.header.code == dtp_type.RESPONSE_CODE_OK:
            original_id = order.order_original_id
            order_detail = self._orders[original_id]
            order_detail.update(order)

        self.on_order_tmp(order)

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
            order_original_id = self.generate_order_id()
            if 'exchange' not in order:
                order['exchange'] = self.get_exchange(order['code'])
            order['price'] = str(order['price'])
            order['order_side'] = order_side
            order['order_type'] = dtp_type.ORDER_TYPE_LIMIT
            order['order_original_id'] = order_original_id

            self._orders[order_original_id] = order

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
            'order_original_id': order_original_id,
            'exchange': exchange,
            'price': price})

        self.trader.send_order(request_id=request_id, **order)
        self._orders[order_original_id] = order
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
        if 'order_exchange_id' not in kw:
            self.logger.warning('未提供 order_exchange_id , 无法撤单')
            return

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
