# -*- coding: utf-8 -*-

import os
import time
import datetime

import threading
import logging
import zmq

import queue
from queue import Queue
import functools
from collections import OrderedDict
import uuid
import json

from fast_trader.dtp import constants
from fast_trader.dtp.constants import dtp_type

from fast_trader.id_pool import _id_pool
from fast_trader.settings import settings, setup_logging
from fast_trader.utils import attrdict
from fast_trader import zmq_context

#import dtp_api
from fast_trader.dtp import dtp_api

# setup_logging()

REQUEST_TIMEOUT = 60


def str2float(s):
    if s == '':
        return 0.
    return float(s)


class OrderResponse:
    """
    报单回报
    """

    @staticmethod
    def from_msg(msg):
        msg = attrdict(msg)
        msg['freeze_amount'] = str2float(msg.freeze_amount)
        msg['price'] = str2float(msg.price)
        return msg


class TradeResponse:
    """
    成交回报
    """

    @staticmethod
    def from_msg(msg):
        msg = attrdict(msg)
        msg['fill_price'] = str2float(msg.fill_price)
        msg['fill_amount'] = str2float(msg.fill_amount)
        msg['clear_amount'] = str2float(msg.clear_amount)
        msg['total_fill_amount'] = str2float(msg.total_fill_amount)
        msg['price'] = str2float(msg.price)
        return msg


class CancellationResponse:
    """
    撤单回报
    """

    @staticmethod
    def from_msg(msg):
        msg = attrdict(msg)
        msg['freeze_amount'] = str2float(msg.freeze_amount)
        return msg


class QueryOrderResponse:
    """
    报单查询相应
    """

    @staticmethod
    def from_msg(msg):
        msg = attrdict(rename_order(msg))
        msg['average_fill_price'] = str2float(msg.average_fill_price)
        msg['clear_amount'] = str2float(msg.clear_amount)
        msg['freeze_amount'] = str2float(msg.freeze_amount)
        msg['price'] = str2float(msg.price)
        msg['total_fill_amount'] = str2float(msg.total_fill_amount)
        return msg


class QueryTradeResponse:
    """
    成交查询相应
    """

    @staticmethod
    def from_msg(msg):
        msg = attrdict(rename_trade(msg))
        msg['fill_price'] = str2float(msg.fill_price)
        msg['fill_amount'] = str2float(msg.fill_amount)
        return msg


class QueryPositionResponse:
    """
    持仓查询相应
    """

    @staticmethod
    def from_msg(msg):
        msg = attrdict(rename_position(msg))
        msg['cost'] = str2float(msg.cost)
        msg['market_value'] = str2float(msg.market_value)
        return msg


def rename_position(item):

    position = {
        'available_quantity': item['availableQuantity'],
        'balance': item['balance'],
        'buy_quantity': item['buyQuantity'],
        'code': item['code'],
        'cost': item['cost'],
        'exchange': item['exchange'],
        'freeze_quantity': item['freezeQuantity'],
        'market_value': item['marketValue'],
        'name': item['name'],
        'sell_quantity': item['sellQuantity']
    }
    position = attrdict(position)

    return position


def rename_trade(item):

    fill = {
        'clear_amount': item['clearAmount'],
        'code': item['code'],
        'exchange': item['exchange'],
        'fill_amount': item['fillAmount'],
        'fill_exchange_id': item['fillId'],
        'fill_price': item['fillPrice'],
        'fill_quantity': item['fillQuantity'],
        'fill_status': item['fillType'],
        'fill_time': item['fillTime'],
        'name': item['name'],
        'order_exchange_id': item['orderExchangeId'],
        'order_original_id': item['orderOriginalId'],
        'order_side': item['side']
    }
    fill = attrdict(fill)

    return fill


def rename_order(kw):

    order = {
        'account_no': kw['accountNo'],
        'average_fill_price': kw['averageFillPrice'],
        'clear_amount': kw['clearAmount'],
        'code': kw['code'],
        'exchange': kw['exchange'],
        'freeze_amount': kw['freezeAmount'],
        'name': kw['name'],
        'order_exchange_id': kw['exchangeId'],
        'order_original_id': kw['originalId'],
        'order_side': kw['side'],
        'order_time': kw['orderTime'],
        'order_type': kw['orderType'],
        'price': kw['price'],
        'quantity': kw['quantity'],
        'status': kw['orderStatus'],
        'status_message': '',
        'total_cancelled_quantity': kw['cancelQuantity'],
        'total_fill_amount': kw['fillAmount'],
        'total_fill_quantity': kw['fillQuantity']
    }
    order = attrdict(order)

    return order


class TimerTask:

    def __init__(self, schedule, some_callable, args=None, kw=None):

        _valid_schedule_types = (datetime.timedelta, datetime.datetime)
        if isinstance(schedule, datetime.timedelta):
            self.type = 'interval'
        elif isinstance(schedule, datetime.datetime):
            self.type = 'once'
        else:
            raise TypeError(f'`schedule` has to be one of'
                            f'{_valid_schedule_types}), got {type(schedule)}')

        self.schedule = schedule
        self.callable = some_callable
        self.args = args or ()
        self.kw = kw or {}

        self._last_time = self.now
        self._finished = False

    @property
    def now(self):
        return datetime.datetime.now()

    def is_finished(self):
        return self._finished

    def time_to_run(self):
        if self._finished:
            return False

        now = datetime.datetime.now()
        if self.type == 'interval':
            target_time = self._last_time + self.schedule
        else:
            target_time = self.schedule

        if now >= target_time:
            return True

        return False

    def execute(self):
        try:
            self.callable(*self.args, **self.kw)
        finally:
            if self.type == 'once':
                self._finished = True
            else:
                # FIXME: should interval task be exed exactly same times
                # as all intervals elapsed
                self._last_time = self.now


class Timer:

    def __init__(self):
        self.tasks = []

    def click(self):

        for task in self.tasks:
            if task.time_to_run():
                task.execute()

    def add_task(self, task):
        self.tasks.append(task)

    def remove_task(self, task):
        raise NotImplementedError


class Dispatcher:

    def __init__(self, **kw):

        self._handlers = {}

        self._req_queue = Queue()
        self._rsp_queue = Queue()
        self._market_queue = Queue()

        self.logger = logging.getLogger('dispatcher')

        self._rsp_processor = None
        self._req_processor = None

        self.timer = Timer()

        self._running = False
        self._service_suspended = False

        self.start()

    def start(self):

        if self._running:
            return

        self._running = True
        self._req_processor = threading.Thread(target=self.process_req)
        self._rsp_processor = threading.Thread(target=self.process_rsp)

        self._req_processor.start()
        self._rsp_processor.start()

    def join(self):
        self._req_processor.join()
        self._rsp_processor.join()

    def process_req(self):

        while self._running:
            mail = self._req_queue.get()
            self.logger.info(mail)
            self.dispatch(mail)

    def process_rsp(self):

        # TODO: poll queues or zmq socks
        while self._running:
            try:
                mail = self._rsp_queue.get(block=False)
                self.logger.info(mail)
                self.dispatch(mail)
            except queue.Empty:
                pass
            except Exception as e:
                self.logger.error(str(e), exc_info=True)

            try:
                mail = self._market_queue.get(block=False)
                self.dispatch(mail)
            except queue.Empty:
                time.sleep(0.0001)
            except Exception as e:
                self.logger.error(str(e), exc_info=True)

            # signal timer event
            self.timer.click()

    def bind(self, handler_id, handler, override=False):
        if not override and handler_id in self._handlers:
            if handler == self._handlers[handler_id]:
                return
            raise KeyError(
                'handler {} already exists!'.format(handler_id))
        self._handlers[handler_id] = handler

    def suspend(self):
        """
        When suspended, ignore all incoming/outgoing messages
        """
        self._service_suspended = True
        self.logger.warning('Mail service is currently suspended.')

    def resume(self):
        self._service_suspended = False

    def put(self, mail):

        if self._service_suspended:
            # ignore all incoming/outgoing messages
            return

        #import pdb;pdb.set_trace()
        handler_id = mail['handler_id']

        if mail.get('sync'):
            return self.dispatch(mail)

        if handler_id.endswith('_req'):
            self._req_queue.put(mail)
        elif handler_id[0].isalpha():
            self._market_queue.put(mail)
        elif handler_id.endswith('_rsp'):
            self._rsp_queue.put(mail)
        else:
            raise Exception('Invalid message: {}'.format(mail))

    def dispatch(self, mail):
        handler_id = mail['handler_id']
        if handler_id in self._handlers:
            return self._handlers[handler_id](mail)


class RestApi():
    
    def __init__(self):
        self._rest_api = None
    
    @property
    def rest_api(self):
        if self._rest_api is None:
            self._rest_api = dtp_api.RestApi()
        return self._rest_api

    def get_accounts(self):
        j = self.rest_api.get_accounts()
        try:
            ret = json.loads(j)
        except:
            raise Exception(j)
        return ret

    def get_capital(self, account_no):
        j = self.rest_api.get_capital(account_no)
        ret = json.loads(j)
        return ret
    
    def get_capitals(self):
        j = self.rest_api.get_capitals()
        ret = json.loads(j)
        return ret

    def get_orders(self, account_no):
        j = self.rest_api.get_orders(account_no)
        ret = json.loads(j)
        ret = sum(ret, [])
        return ret

    def get_trades(self, account_no):
        j = self.rest_api.get_trades(account_no)
        ret = json.loads(j)
        ret = sum(ret, [])
        return ret

    def get_positions(self, account_no):
        j = self.rest_api.get_positions(account_no)
        ret = json.loads(j)
        ret = sum(ret, [])
        return ret

    def place_order(self, order_req):
        self.rest_api.place_order(order_req)

    def place_batch_order(self, batch_order_req, account_no):
        self.rest_api.place_batch_order(batch_order_req, account_no)

    def cancel_order(self, order_cancelation_req):
        self.rest_api.cancel_order(order_cancelation_req)
    
    
class DTP(dtp_api.Trader, RestApi):

    def __init__(self, dispatcher):
        RestApi.__init__(self)
        self.accounts = [d['cashAccountNo'] for d in self.get_accounts()]

        dtp_api.Trader.__init__(self, self.accounts)
        
        self.dispatcher = dispatcher

        self.logger = logging.getLogger('dtp')

        self._counter_report_thread = threading.Thread(
            target=self._recv)
    
    def _recv(self):
        ctx = zmq_context.manager.context
        sock = ctx.socket(zmq.SUB)
        port = self._get_bound_port()
        sock.connect(f'tcp://127.0.0.1:{port}')
        sock.subscribe('')
        
        while True:
            mail = attrdict(sock.recv_json())
            self.dispatcher.put(mail)

    def start_counter_report(self):
        self.start()
        self._counter_report_thread.start()


class Order:

    exchange = dtp_type.EXCHANGE_SH_A
    code = ''
    price = ''
    quantity = 0
    order_side = dtp_type.ORDER_SIDE_BUY
    order_type = dtp_type.ORDER_TYPE_LIMIT

    def __getitem__(self, key):
        return getattr(self, key)


class Trader:

    def __init__(self, dispatcher=None, trade_api=None, trader_id=0):

        self.dispatcher = dispatcher
        self._trade_api = trade_api

        self.trader_id = trader_id

        self._started = False

        self._account_no = ''
        self._token = ''
        self._logined = False

        self._position_results = []
        self._trade_results = []
        self._order_results = []

        self._strategies = []
        self._strategy_dict = OrderedDict()

        self.__api_bound = False
        
        self.logger = logging.getLogger('trader')

    def log_req(func):

        @functools.wraps(func)
        def wrapper(*args, **kw):
            args_ = args[1:]
            logger = args[0].logger
            logger.info(
                f'{func.__name__}<args={args_}, kwargs={kw}')
            return func(*args, **kw)
        return wrapper

    def start(self):

        if self._started:
            return

        if self.dispatcher is None:
            self.dispatcher = Dispatcher()

        if self._trade_api is None:
            self._trade_api = DTP(self.dispatcher)

        self._bind()

        self._trade_api.start_counter_report()

        self._started = True

    def _bind(self):

        if not self.__api_bound:

            dispatcher, _trade_api = self.dispatcher, self._trade_api

            for api_id in constants.RSP_API_NAMES:
                dispatcher.bind(f'{self.account_no}_{api_id}_rsp',
                                self._on_response)

#            for api_id in constants.REQ_API_NAMES:
#                api_name = constants.REQ_API_NAMES[api_id]
#                handler = getattr(_trade_api, api_name)
#                dispatcher.bind(f'{api_id}_req', handler)

            self.__api_bound = True
        else:
            self.logger.warning('Trader alreadly has been bound')

    def _set_number(self, number, max_number=None):
        """
        预留接口

        如要同时启动多个trader实例, 为了保证请求/报单编号的唯一性，
        可以根据trader编号, 为其分配独立的取值范围
        """
        self._number = number
        if max_number:
            self._max_number = max_number

    def _generate_initial_id(self, strategy):
        """
        计算初始编号
        """
        strategy_id = strategy.strategy_id
        id_range = _id_pool.get_strategy_range_per_trader(
            strategy_id, self.trader_id)

        strategy._id_range = id_range
        strategy._id_whole_range = _id_pool.get_strategy_whole_range(
            strategy_id)

        initial_id = id_range[0]
        self.logger.debug('初始请求编号 策略={} {}'.format(
            strategy_id, initial_id))

        setattr(self, '_order_id_{}'.format(strategy_id), initial_id)
        setattr(self, '_request_id_{}'.format(strategy_id), initial_id)

    def generate_request_id(self, number=1):
        """
        请求id，保证当日不重复
        """
        request_id = str(uuid.uuid1())
        return request_id

    def generate_order_id(self, number):
        """
        用户报单编号，保证当日不重复
        """
        name = '{}_{}'.format('_order_id', number)

        order_id = getattr(self, name)
        setattr(self, name, order_id + 1)
        return str(order_id)

    def add_strategy(self, strategy):
        self._strategies.append(strategy)
        self._strategy_dict[strategy.strategy_id] = strategy
        self._generate_initial_id(strategy)

    def remove_strategy(self, strategy):
        # TODO: keep one only
        self._strategies.remove(strategy)
        self._strategy_dict.pop(strategy.strategy_id)
    
    def set_account_no(self, account_no):
        self._account_no = account_no

    def _on_response(self, mail):

        api_id = mail['api_id']

        # TODO: might be opt out
        if 'header' in mail['content']:
            mail['content']['body']['message'] =\
                mail['content']['header']['message']
        
        #import pdb;pdb.set_trace()

        if api_id == constants.LOGIN_ACCOUNT_RESPONSE:
            self.on_login(mail)
        elif api_id == constants.LOGOUT_ACCOUNT_RESPONSE:
            self.on_logout(mail)
        else:
            for ea in self._strategies:
                if ea.started and ea._check_owner(mail['content']):
                    getattr(
                        ea, 
                        constants.RSP_API_NAMES[api_id])(mail['content'])

    @property
    def account_no(self):
        return self._account_no

    @property
    def logined(self):
        return self._logined

    def on_login(self, msg):
        raise NotImplementedError

    def on_logout(self, mail):
        raise NotImplementedError

    def login(self, account_no, password, sync=True, **kw):
        """
        登录
        """
        self.set_account_no(account_no)
        self._logined = True
        self.start()

    def logout(self, **kw):
        """
        登出暂为空操作
        """
        pass

    @log_req
    def place_order(self, **order):
        """
        报单委托
        """
        order_req = dtp_api.OrderReq()
        order_req.account_no = self.account_no
        order_req.code = order['code']
        order_req.order_original_id = order['order_original_id']
        order_req.exchange = order['exchange']
        order_req.order_type = order['order_type']
        order_req.order_side = order['order_side']
        order_req.price = order['price']
        order_req.quantity = order['quantity']

        self._trade_api.place_order(order_req)

    @log_req
    def place_batch_order(self, orders):
        """
        批量下单
        """
        batch_order_req = []
        for order in orders:
            order_req = dtp_api.OrderReq()
            batch_order_req.append(order_req)
            order_req.code = order['code']
            order_req.order_original_id = order['order_original_id']
            order_req.exchange = order['exchange']
            order_req.order_type = order['order_type']
            order_req.order_side = order['order_side']
            order_req.price = order['price']
            order_req.quantity = order['quantity']

        self._trade_api.place_batch_order(batch_order_req, self.account_no)

    @log_req
    def cancel_order(self, order_exchange_id, exchange, **kw):
        """
        撤单
        """
        order_cancelation_req = dtp_api.OrderCancelationReq()
        order_cancelation_req.account_no = self.account_no
        order_cancelation_req.exchange = exchange
        order_cancelation_req.order_exchange_id = order_exchange_id
        return self._trade_api.cancel_order(order_cancelation_req)

    @log_req
    def query_orders(self, **kw):
        """
        查询订单
        """
        mail = attrdict()
        orders = self._trade_api.get_orders(account_no=self.account_no)
        mail['body'] = orders
        return mail

    @log_req
    def query_trades(self, **kw):
        """
        查询成交
        """
        mail = attrdict()
        trades = self._trade_api.get_trades(account_no=self.account_no)
        mail['body'] = trades
        return mail

    @log_req
    def query_positions(self, **kw):
        """
        查询持仓
        """
        mail = attrdict()
        positions = self._trade_api.get_positions(account_no=self.account_no)
        mail['body'] = positions
        return mail

    @log_req 
    def query_capital(self, **kw):
        """
        查询账户资金
        """
        mail = attrdict()
        mail['body'] = self._trade_api.get_capital(
            account_no=self.account_no)[0]
        return mail

    def query_ration(self, **kw):
        """
        查询配售权益
        """
        raise NotImplementedError
