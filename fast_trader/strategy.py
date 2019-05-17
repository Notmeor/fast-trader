# -*- coding: utf-8 -*-

import os
import time
import datetime
import threading
import logging
import collections
import pandas as pd

from fast_trader.dtp_trade import DTP, Trader, Dispatcher
from fast_trader.dtp_quote import (Transaction, Snapshot,
                                   MarketOrder, Index, OrderQueue)

from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.dtp_trade import (OrderResponse, TradeResponse,
                                   CancellationResponse,
                                   QueryOrderResponse, QueryTradeResponse,
                                   QueryPositionResponse)

from fast_trader.models import StrategyStatus

from fast_trader.settings import settings, Session
from fast_trader.utils import (timeit, message2tuple, attrdict, as_wind_code,
                               get_current_ts)

from fast_trader.ledger import LedgerWriter


# 委托状态添加 `SUBMITTED` : 报单本地已发送
dtp_type.ORDER_STATUS_SUBMITTED = -1


class Market:

    def __init__(self):
        self._strategies = []

    def add_strategy(self, strategy):
        self._strategies.append(strategy)

    def remove_strategy(self, strategy):
        self._strategies.remove(strategy)
    
    @staticmethod
    def has_subscribed(strategy, message):
        ds_name = message['api_id']

        for ds in strategy.subscribed_datasources:
            if ds.name == ds_name:
                raise NotImplementedError

    def on_quote_message(self, message):
        for ea in self._strategies:
            ea.on_quote_message(message)


def to_timeint(dt):
    return (dt.hour * 10000000 + dt.minute * 100000 +
            dt.second * 1000 + int(dt.microsecond / 1000))


def as_order_msg(order):
    drc = {
        dtp_type.ORDER_SIDE_BUY: '买入',
        dtp_type.ORDER_SIDE_SELL: '卖出',
    }[order.order_side]

    status = {
        dtp_type.ORDER_STATUS_PLACING: '委托成功',
        dtp_type.ORDER_STATUS_PLACED: '已申报',
        dtp_type.ORDER_STATUS_FAILED: '废单',
    }.get(order.status, str(order.status))

    err = ''
    if order.status == dtp_type.ORDER_STATUS_FAILED:
        err = f'{order.message}, '

    msg = f'{status}, {err}委托编号={order.order_exchange_id}, '\
        f'证券代码={order.code}, 方向={drc}, '\
        f'委托价格={order.price}, 委托数量={order.quantity}'
    return msg


def as_trade_msg(trade):
    drc = {
        dtp_type.ORDER_SIDE_BUY: '买入',
        dtp_type.ORDER_SIDE_SELL: '卖出',
    }[trade.order_side]

    msg = f'成交回报, 委托编号={trade.order_exchange_id}, '\
        f'证券代码={trade.code}, 方向={drc}, '\
        f'委托价格={trade.fill_price}, 本次成交数量={trade.fill_quantity}, '\
        f'已成交数量={trade.total_fill_quantity}, 总委托数量={trade.quantity}'
    return msg


class StrategyWatchMixin:
    """
    策略运行状态监控
    """

    def _mark_strategy_as_started(self):

        msg = {
            'pid': os.getpid(),
            'account_no': self.trader.account_no,
            'strategy_id': self.strategy_id,
            'strategy_name': self.strategy_name,
            'start_time': datetime.datetime.now(
                ).strftime('%Y%m%d %H:%M:%S.%f'),
            'token': '',
            'running': True,
            'last_heartbeat': get_current_ts()}

        session = Session()
        last_status = (
            session
            .query(StrategyStatus)
            .filter_by(strategy_id=msg['strategy_id'])
            .first()
        )
        if last_status is None:
            status = StrategyStatus.from_msg(msg)
            session.add(status)
        else:
            for k, v in msg.items():
                setattr(last_status, k, v)

        session.commit()
        session.close()

        if not hasattr(self, '_heartbeat_thread'):
            self._heartbeat_thread = threading.Thread(
                target=self._send_heartbeat)
            self._heartbeat_thread.start()

    def _send_heartbeat(self):
        # FIXME: scoped session
        if not hasattr(self, '_session'):
            self._session = Session()

        while self._started:
            ts = get_current_ts()

            (self._session
                .query(StrategyStatus)
                .filter_by(strategy_id=self.strategy_id)
                .update({'last_heartbeat': ts}))

            self._session.commit()
            time.sleep(0.5)

    def _check_strategy_status(self):
        """
        验证strategy_id是否可用
        """
        session = Session()
        res = (
            session
            .query(StrategyStatus)
            .filter_by(strategy_id=self.strategy_id)
            .all()
        )
        if len(res) > 0:
            stats = res[0]
            if stats.is_running():
                err_msg = (f'strategy_id={stats.strategy_id}的' +
                           f'策略正在运行: {stats.strategy_name}')
                self.logger.error(f'策略初始化失败: {err_msg}')
                raise RuntimeError(err_msg)
        session.close()


class Strategy(StrategyWatchMixin):

    strategy_name = 'unnamed'
    strategy_id = -1
    trader_id = 1

    def __init__(self, strategy_id=None):

        self.logger = logging.getLogger(
            f'strategy<id={strategy_id};name={self.strategy_name}>')

        if self.strategy_id < 0 or not isinstance(self.strategy_id, int):
            raise RuntimeError(
                f'`strategy_id`应为非0整数，当前取值：{self.strategy_id}')

        if strategy_id is not None:
            self.strategy_id = strategy_id

        # 验证是否已存在同id的策略正在运行
        self._check_strategy_status()

        self._started = False

        self._positions = {}
        self._orders = collections.defaultdict(attrdict)
        self._trades = collections.defaultdict(attrdict)
        self._request_id_history = []

        self.subscribed_datasources = []

        # order_exchange_id -> order_original_id
        self._order_id_mapping = {}

        self._ledger_writer = LedgerWriter(name=f'strategy_{self.strategy_id}')

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

    def set_initial_capital(self, value):
        view = self.get_account_view()
        if view.balance == 0. and view.costs == 0.:
            self.add_cash(value)
        else:
            self.logger.info('已存在策略账户记录, 初始资金设置无效')

    def set_initial_positions(self, positions):
        """
        设置策略初始持仓
        """
        raise NotImplementedError

    def start(self):

        self.trader.start()

        self._account_no = settings['account']
        self.trader.login(
            account=settings['account'],
            password=settings['password'],
            request_id=self.generate_request_id(),
            sync=True)

        if self.trader.logined:
            self._started = True
            self.on_start()
            self.logger.info('策略启动成功')

            # 更新策略状态的本地记录
            self._mark_strategy_as_started()

            # 启动行情线程
            self.start_market()

            return {'ret_code': 0, 'data': None}

        else:
            err_msg = '策略启动失败 账户<{}>未成功登录'.format(self.account_no)
            self.logger.warning(err_msg)
            return {'ret_code': -1, 'err_msg': err_msg}

    def stop(self):
        self.remove_self()
        self.logger.info('策略已终止')

    def remove_self(self):
        self.trader.remove_strategy(self)
        self.market.remove_strategy(self)
        self._started = False

    def start_market(self):

        for ds in self.subscribed_datasources:
            ds.start()

    def add_cash(self, value):
        """
        策略账户入金
        """
        self._ledger_writer.write_capital_change_record(value)
        self.logger.info(f'策略账户入金: {value}')

    def withdraw_cash(self, value):
        """
        策略账户出金
        """
        self._ledger_writer.write_capital_change_record(-value)
        self.logger.info(f'策略账户出金: {value}')

    @classmethod
    def get_strategy_name(cls):
        if cls.strategy_name == 'unnamed':
            return cls.__name__
        return cls.strategy_name

    @property
    def account_no(self):
        return self.trader._account_no

    def get_account_view(self, code=None):
        """
        获取策略账户概要
        """
        acc = self._ledger_writer.accountant
        if code is None:
            view = acc.get_general_account_view()
        else:
            view = acc.get_account_view_by_code(code)
        return view

    def get_account_history(self, code=None):
        """
        获取策略账户历史概要
        """
        acc = self._ledger_writer.accountant
        if code is None:
            view = acc.get_general_account_history_stats()
        else:
            view = acc.get_account_history_stats_by_code(code)
        return view

    def get_account_records(self):
        """
        获取策略账户流水
        """
        return self._ledger_writer.accountant._records

    def generate_order_id(self):
        return self.trader.generate_order_id(self.strategy_id)

    def generate_request_id(self):
        request_id = self.trader.generate_request_id(self.strategy_id)
        # 部分响应数据，如CANCEL RESPONSE无`order_original_id`,
        # 需要记录request_id用来确定此类消息的策略归属
        # 但在使用rest api接口时，无法指定request_id
        self._request_id_history.append(request_id)
        return request_id

    def _check_owner(self, obj):
        """
        判断该查询响应数据是否属于当前策略
        """

        id_range = self._id_whole_range

        def _check_order_id(o):
            _id = o['order_original_id']
            if int(_id) in id_range:
                return True
            return False

        if 'order_original_id' in obj:
            return _check_order_id(obj)
        elif 'body' in obj:
            if 'order_original_id' in obj['body']:
                return _check_order_id(obj['body'])
            elif 'order_exchange_id' in obj['body']:
                # cancel response has 'order_exchange_id'
                # but no 'order_original_id'
                exchange_id = obj['body']['order_exchange_id']
                if exchange_id in self._order_id_mapping:
                    return True

            if obj['header']['request_id'] in self._request_id_history:
                return True

        return False

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

    def get_account_orders(self):
        """
        查询账户报单
        """
        orders = self._get_all_pages(self.trader.query_orders)
        ret = [QueryOrderResponse.from_msg(order) for order in orders]
        return ret

    def get_account_open_orders(self):
        """
        查询账户未成交(可撤)报单

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
        orders = self.get_account_orders()
        open_orders = [order for order in orders if order['status'] < 4]
        return open_orders

    def get_account_trades(self):
        """
        查询账户成交
        """
        trades = self._get_all_pages(self.trader.query_trades)
        ret = [QueryTradeResponse.from_msg(trade) for trade in trades]
        return ret

    def get_account_positions(self):
        """
        查询账户持仓
        """
        positions = self._get_all_pages(self.trader.query_positions)
        ret = [QueryPositionResponse.from_msg(pos) for pos in positions]
        return ret

    def get_orders(self):
        """
        查询报单
        """
        orders = self.get_account_orders()
        orders = [order for order in orders if self._check_owner(order)]
        return orders

    def get_open_orders(self):
        """
        查询未成交报单
        """
        # TODO: 通过参数查询
        # TODO: 默认使用本地委托记录
        orders = self.get_account_open_orders()
        orders = [order for order in orders if self._check_owner(order)]
        return orders

    def get_trades(self):
        """
        查询成交（同步）
        """
        trades = self.get_account_trades()
        trades = [trade for trade in trades if self._check_owner(trade)]
        return trades

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

    def get_positions(self):
        """
        查询策略持仓
        """
        # FIXME: return account positions when bound to main account
        positions = self._ledger_writer.accountant._positions
        ret = []
        for code, pos in positions.items():
            p = attrdict()
            ret.append(p)
            p['available_quantity'] = pos.sellable_quantity
            p['balance'] = pos.quantity
            p['buy_quantity'] = None
            p['sell_quantity'] = None
            p['code'] = code
            p['cost'] = None
            p['exchange'] = self.get_exchange(code)
            p['freeze_quantity'] = pos.quantity - pos.sellable_quantity
            p['market_value'] = pos.quantity * pos.price
            p['name'] = None
        return ret

    def get_exchange(self, code):
        """
        返回交易所代码
        """
        if code.startswith('6'):
            return dtp_type.EXCHANGE_SH_A
        else:
            return dtp_type.EXCHANGE_SZ_A

    def get_all_trade_records(self):
        """
        获取所有历史交易记录
        """
        acc = self._ledger_writer.accountant
        df = pd.DataFrame(list(map(lambda x: x.__dict__, acc._records)))
        trades = df\
            .query("subject == 'transaction' and category == 'security'")\
            .loc[:, ['code', 'quantity', 'price', 'localtime']]\
            .reset_index(drop=True)
        return trades

    def add_datasource(self, datasource):
        """
        添加行情数据源
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

        # 估值行情
        if api_id in ['tick_feed', 'trade_feed']:
            self._ledger_writer.write_market_record(message)

        if api_id == 'trade_feed':
            data = message['content']
            self.on_market_trade(data)

        elif api_id == 'tick_feed':
            data = message['content']
            self.on_market_snapshot(data)

        elif api_id == 'order_feed':
            data = message['content']
            self.on_market_order(data)

        elif api_id == 'queue_feed':
            data = message['content']
            self.on_market_queue(data)

        elif api_id == 'index_feed':
            data = message['content']
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

    def on_market_trade(self, market_trade):
        """
        逐笔成交行情
        """
        pass

    def on_market_order(self, market_order):
        """
        逐笔报单行情(上交所无该数据推送)
        """
        pass

    def on_market_index(self, market_index):
        """
        指数行情
        """
        pass

    def _on_order_submitted(self, order):
        """
        响应本地报单提交

        主要用来维护资金状态，本地报单后，立即冻结委托资金
        """
        # FIXME: `price` data type
        self._ledger_writer.write_order_record(order)

    def _on_trade(self, msg):
        """
        成交回报
        """
        trade = TradeResponse.from_msg(msg.body)

        self.logger.info(as_trade_msg(trade))

        if msg.header.code == dtp_type.RESPONSE_CODE_OK:
            original_id = trade.order_original_id
            # FIXME: strategy might start before trade responses and after
            # order responses
            order_detail = self._orders[original_id]
            order_detail.update(trade)

            # 成交时，可能只会收到成交回报，而不会收到报单回报
            # 此时手动更新order_status
            if 0 < trade.total_fill_quantity < trade.quantity:
                if order_detail.status not in [
                        dtp_type.ORDER_STATUS_PARTIAL_CANCELLED]:
                    order_detail['status'] = \
                        dtp_type.ORDER_STATUS_PARTIAL_FILLED
            elif trade.total_fill_quantity == trade.quantity:
                order_detail['status'] = dtp_type.ORDER_STATUS_FILLED

        if msg.body.fill_status != 1:
            self.logger.error(msg)
        else:
            self._trades[trade.order_original_id] = trade

            # 写入账户流水
            self._ledger_writer.write_trade_record(trade)

            self.on_trade(trade)

    def on_trade(self, data):
        """
        用户策略覆盖此方法以处理成交回报
        """
        pass

    def _on_order(self, msg):
        """
        订单回报
        """
        order = OrderResponse.from_msg(msg.body)

        self._order_id_mapping[order['order_exchange_id']] =\
            order['order_original_id']

        self.logger.info(as_order_msg(order))

        # if msg.header.code == dtp_type.RESPONSE_CODE_OK:
        original_id = order.order_original_id
        order_detail = self._orders[original_id]

        # NOTE: 委托回报可能晚于成交回报
        # 抛弃过期委托回报
        try:
            if order_detail.status > order.status:
                self.logger.warning(f'Expired order response: {order}')
                return
        except:
            import pdb;pdb.set_trace()

        order_detail.update(order)

        self._ledger_writer.write_order_record(order)

        self.on_order(order)

    def on_order(self, data):
        """
        用户策略覆盖此方法以处理委托回报
        """
        pass

    def _on_batch_order_submission(self, msg):
        """
        批量委托响应
        """
        pass

    def _on_order_query(self, orders):
        """
        报单查询
        """
        pass

    def _on_order_cancelation_submission(self, msg):
        """
        撤单提交响应
        """
        self.logger.info(
            f'{msg.body.message}, 委托编号={msg.body.order_exchange_id}')

    def _on_order_cancelation(self, msg):
        """
        撤单确认回报
        """
        data = CancellationResponse.from_msg(msg.body)

        self.logger.info(f'已撤单, 委托编号={data.order_exchange_id}')

        if msg.header.code == dtp_type.RESPONSE_CODE_OK:
            original_id = data.order_original_id
            order_detail = self._orders[original_id]
            order_detail.update(data)

        self._ledger_writer.write_order_record(data)

        self.on_order_cancelation(data)

    def on_order_cancelation(self, msg):
        """
        用户策略覆盖此方法以处理撤单成功回报
        """
        pass

    def _on_compliance_report(self, report):
        """
        风控回报
        """
        pass

    def _store_order(self, order):
        order['status'] = dtp_type.ORDER_STATUS_SUBMITTED
        order['placed_localtime'] = to_timeint(datetime.datetime.now())
        order = attrdict(order)
        self._orders[order.order_original_id] = order

        # 本地报单后，发出一个order submitted事件
        self._on_order_submitted(order)

        return order

    def _insert_order(self, **kw):
        request_id = self.generate_request_id()
        order_original_id = self.generate_order_id()
        exchange = kw['exchange'] or self.get_exchange(kw['code'])
        price = str(kw['price'])

        order = kw.copy()
        order.update({
            'order_original_id': order_original_id,
            'exchange': exchange,
            'price': price,
        })

        self.trader.place_order(request_id=request_id, **order)

        order = self._store_order(order)

        return order

    def _insert_many(self, order_side, orders):
        request_id = self.generate_request_id()
        for order in orders:
            order_original_id = self.generate_order_id()
            if 'exchange' not in order:
                order['exchange'] = self.get_exchange(order['code'])
            order['order_side'] = order_side
            order['order_type'] = dtp_type.ORDER_TYPE_LIMIT
            order['order_original_id'] = order_original_id
            order['price'] = str(order['price'])

        self.trader.place_batch_order(request_id=request_id, orders=orders)

        for order in orders:
            order = self._store_order(order)

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


class StrategyFactory:
    """
    演示策略实例化流程
    """
    def __init__(self, trader_id=0, factory_settings=None):

        # FIXME: settings should be independent for each factory instance
        if factory_settings is not None:
            settings.set(factory_settings)

        self.trader_id = trader_id

        # 用于 trader 与 dtp通道 以及 策略实例 间的消息分发
        # 将所有行情数据与柜台回报在同一个线程中进行分发
        self.dispatcher = Dispatcher()

        # dtp通道
        self.dtp = DTP(self.dispatcher)

        # 行情通道
        self.market = Market()

        self.trader = Trader(self.dispatcher, self.dtp, trader_id)

    def generate_strategy(self, StrategyCls, strategy_id):

        strategy = StrategyCls(strategy_id)

        strategy.set_trader(self.trader)

        strategy.set_dispatcher(self.dispatcher)

        strategy.set_market(self.market)

        return strategy

    def remove_strategy(self, strategy):
        self.market.remove_strategy(strategy)
        strategy.trader.remove_strategy(strategy)
