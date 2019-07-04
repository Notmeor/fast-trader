# -*- coding: utf-8 -*-

import os
import time
import datetime
import queue
import multiprocessing
import threading
import logging


from collections import OrderedDict
import uuid
import json

from fast_trader.dtp import dtp_api_id

from fast_trader.dtp import ext_api_pb2 as dtp_struct
from fast_trader.dtp import ext_type_pb2 as dtp_type

from fast_trader.id_pool import _id_pool
from fast_trader.utils import timeit, attrdict, message2dict, Mail
from fast_trader.settings import settings, setup_logging

import dtp_api

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
        ret = msg  # .copy()
        ret['freeze_amount'] = str2float(msg.freeze_amount)
        ret['price'] = str2float(msg.price)
        return ret


class TradeResponse:
    """
    成交回报
    """

    @staticmethod
    def from_msg(msg):
        ret = msg
        ret['fill_price'] = str2float(msg.fill_price)
        ret['fill_amount'] = str2float(msg.fill_amount)
        ret['clear_amount'] = str2float(msg.clear_amount)
        ret['total_fill_amount'] = str2float(msg.total_fill_amount)
        ret['price'] = str2float(msg.price)
        return ret


class CancellationResponse:
    """
    撤单回报
    """

    @staticmethod
    def from_msg(msg):
        ret = msg
        ret['freeze_amount'] = str2float(msg.freeze_amount)
        return ret


class QueryOrderResponse:
    """
    报单查询相应
    """

    @staticmethod
    def from_msg(msg):
        ret = msg
        ret['average_fill_price'] = str2float(msg.average_fill_price)
        ret['clear_amount'] = str2float(msg.clear_amount)
        ret['freeze_amount'] = str2float(msg.freeze_amount)
        ret['price'] = str2float(msg.price)
        ret['total_fill_amount'] = str2float(msg.total_fill_amount)
        return ret


class QueryTradeResponse:
    """
    成交查询相应
    """

    @staticmethod
    def from_msg(msg):
        ret = msg  # .copy()
        ret['fill_price'] = str2float(msg.fill_price)
        ret['fill_amount'] = str2float(msg.fill_amount)
        return ret


class QueryPositionResponse:
    """
    持仓查询相应
    """

    @staticmethod
    def from_msg(msg):
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


class Mailbox:
    
    def __init__(self):
        self._q = multiprocessing.Queue()
    
    def get(self, *args, **kw):
        return self._q.get(*args, **kw)
    
    def put(self, item, *args, **kw):
        self._q.put(item, *args, **kw)



class Dispatcher:

    def __init__(self, **kw):

        self._handlers = {}

        self._req_queue = queue.Queue()
        self._rsp_queue = queue.Queue()
        self._market_queue = queue.Queue()

        self.logger = logging.getLogger('dispatcher')

        self._rsp_processor = None
        self._req_processor = None

        self.timer = Timer()
        
        self.mailbox = Mailbox()

        self._running = False
        self._service_suspended = False

        self.start()

    def start(self):

        if self._running:
            return

        self._running = True

        self._rsp_processor = threading.Thread(target=self.process_rsp)

        self._rsp_processor.start()

    def join(self):
        self._req_processor.join()
        self._rsp_processor.join()

    def process_req(self):

        while self._running:
            mail = self._req_queue.get()
            self.dispatch(mail)

    def process_rsp(self):

        while self._running:
            try:
                mail = self.mailbox.get(timeout=0.0001)
                self.dispatch(mail)
            except queue.Empty:
                pass
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

        handler_id = mail['handler_id']

        if mail.get('sync'):
            return self.dispatch(mail)

        if handler_id.endswith('_req'):
            self._req_queue.put(mail)
        elif handler_id[0].isalpha():
            self._market_queue.put(mail)
        elif handler_id.endswith('_rsp'):
            #self._rsp_queue.put(mail)
            self.mailbox.put(mail)
        else:
            raise Exception('Invalid message: {}'.format(mail))

    def dispatch(self, mail):
        handler_id = mail['handler_id']
        if handler_id in self._handlers:
            return self._handlers[handler_id](mail)


class DTPType:

    api_map = {
        'CANCEL_REPORT': 'CancellationReport',
        'CANCEL_ORDER': 'CancelOrder',
        'CANCEL_RESPONSE': 'CancelResponse',
        'FILL_REPORT': 'FillReport',
        'LOGIN_ACCOUNT_REQUEST': 'LoginAccountRequest',
        'LOGIN_ACCOUNT_RESPONSE': 'LoginAccountResponse',
        'LOGOUT_ACCOUNT_REQUEST': 'LogoutAccountRequest',
        'LOGOUT_ACCOUNT_RESPONSE': 'LogoutAccountResponse',
        'PLACE_REPORT': 'PlacedReport',
        'PLACE_BATCH_ORDER': 'PlaceBatchOrder',
        'PLACE_BATCH_RESPONSE': 'PlaceBatchResponse',
        'PLACE_ORDER': 'PlaceOrder',
        'QUERY_CAPITAL_REQUEST': 'QueryCapitalRequest',
        'QUERY_CAPITAL_RESPONSE': 'QueryCapitalResponse',
        'QUERY_FILLS_REQUEST': 'QueryFillsRequest',
        'QUERY_FILLS_RESPONSE': 'QueryFillsResponse',
        'QUERY_ORDERS_REQUEST': 'QueryOrdersRequest',
        'QUERY_ORDERS_RESPONSE': 'QueryOrdersResponse',
        'QUERY_POSITION_REQUEST': 'QueryPositionRequest',
        'QUERY_POSITION_RESPONSE': 'QueryPositionResponse',
        'QUERY_RATION_REQUEST': 'QueryRationRequest',
        'QUERY_RATION_RESPONSE': 'QueryRationResponse'
    }

    proto_structs = {getattr(dtp_api_id, k): getattr(dtp_struct, v)
                     for k, v in api_map.items()}

    @classmethod
    def get_proto_type(cls, api_id):
        return cls.proto_structs[api_id]


class DTP(dtp_api.Trader):

    def __init__(self, dispatcher):

        self.rest_api = dtp_api.RestApi()
        self.accounts = [d['cashAccountNo'] for d in self.get_accounts()]

        dtp_api.Trader.__init__(self, self.accounts)
        self.dispatcher = dispatcher

        self.logger = logging.getLogger('dtp')

    def start_counter_report(self):
        self.start()
        self._counter_report_thread = threading.Thread(
            target=self.process_counter_report)
        self._counter_report_thread.start()

    def on_message_bytes(self, header_bytes, body_bytes):

        header = dtp_struct.ReportHeader()
        header.ParseFromString(header_bytes)

        rsp_type = DTPType.get_proto_type(header.api_id)
        body = rsp_type()
        try:
            body.ParseFromString(body_bytes)
        except:
            err_msg = f'err bytes: {body_bytes}, type={type(body_bytes)}, api_id={header.api_id}'
            print(err_msg)
            self.logger.error(err_msg)

        mail = Mail(
            api_id=header.api_id,
            api_type='rsp',
            handler_id=f'{body.account_no}_{header.api_id}_rsp',
        )

        mail['header'] = message2dict(header)
        mail['body'] = message2dict(body)

        #self.logger.info(mail)

        self.dispatcher.put(mail)

    def get_accounts(self):
        j = self.rest_api.get_accounts()
        ret = json.loads(j)
        return ret

    def get_capital(self, account_no):
        j = self.rest_api.get_capital(account_no)
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


class DTP_(dtp_api.Trader):
    
    def __init__(self, dispatcher):

        self.rest_api = dtp_api.RestApi()
        self.accounts = [d['cashAccountNo'] for d in self.get_accounts()]

        dtp_api.Trader.__init__(self, self.accounts)
        self.dispatcher = dispatcher

        self.logger = logging.getLogger('dtp')

        self._counter_report_thread = threading.Thread(
            target=self.process_counter_report)
        
        print(f'init thread: {threading.get_ident()}')

    def start_counter_report(self):
        self.start()
        self._counter_report_thread.start()

    def on_message_bytes(self, header_bytes, body_bytes):

        header = dtp_struct.ReportHeader()
        header.ParseFromString(header_bytes)

        rsp_type = DTPType.get_proto_type(header.api_id)
        body = rsp_type()
        try:
            body.ParseFromString(body_bytes)
        except:
            err_msg = f'err bytes: {body_bytes}, type={type(body_bytes)}, api_id={header.api_id}'
            print(err_msg)
            self.logger.error(err_msg)

        mail = Mail(
            api_id=header.api_id,
            api_type='rsp',
            handler_id=f'{body.account_no}_{header.api_id}_rsp',
        )

        mail['header'] = message2dict(header)
        mail['body'] = message2dict(body)
        print(f'\rmessage: {mail.header.message}, {threading.get_ident()}', end='')

    def get_accounts(self):
        j = self.rest_api.get_accounts()
        ret = json.loads(j)
        return ret

    def get_capital(self, account_no):
        j = self.rest_api.get_capital(account_no)
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


class CounterReportProducer:
    
    def __init__(self):
        self.mailbox = None
        self.proc = None
    
    def set_mailbox(self, mailbox):
        self.mailbox = mailbox
    
    def produce(self):
        class Counter(dtp_api.Trader):
        
            def __init__(self):
        
                self.rest_api = dtp_api.RestApi()
                self.accounts = [d['cashAccountNo'] 
                                 for d in self.get_accounts()]
                dtp_api.Trader.__init__(self, self.accounts)
            
            def set_mailbox(self, mailbox):
                self.mailbox = mailbox
            
            def get_accounts(self):
                j = self.rest_api.get_accounts()
                ret = json.loads(j)
                return ret
        
            def on_message_bytes(self, header_bytes, body_bytes):
        
                header = dtp_struct.ReportHeader()
                header.ParseFromString(header_bytes)
        
                rsp_type = DTPType.get_proto_type(header.api_id)
                body = rsp_type()
                try:
                    body.ParseFromString(body_bytes)
                except:
                    err_msg = f'err bytes: {body_bytes}, type={type(body_bytes)}, api_id={header.api_id}'
                    print(err_msg)
                    self.logger.error(err_msg)
        
                mail = Mail(
                    api_id=header.api_id,
                    api_type='rsp',
                    handler_id=f'{body.account_no}_{header.api_id}_rsp',
                )
        
                mail['header'] = message2dict(header)
                mail['body'] = message2dict(body)
                print('counter mail:', mail)
                self.mailbox.put(mail)
        
        counter = Counter()
        counter.set_mailbox(self.mailbox)
        counter.start()
        counter.process_counter_report()
    
    def start(self):
        self.proc = multiprocessing.Process(target=self.produce)
        self.proc.start()


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

        # order_exchange_id -> order_original_id
        self._order_id_mapping = {}

        self.logger = logging.getLogger('trader')
        self.logger.debug('初始化 process_id={}'.format(os.getpid()))
    
    def start_counter_report(self):
        self.counter_report_producer = CounterReportProducer()
        self.counter_report_producer.set_mailbox(self.dispatcher.mailbox)
        self.counter_report_producer.start()

    def start(self):

        if self._started:
            return

        if self.dispatcher is None:
            self.dispatcher = Dispatcher()

        if self._trade_api is None:
            self._trade_api = DTP(self.dispatcher)

        self._bind()

        #self._trade_api.start_counter_report()
        self.start_counter_report()

        self._started = True

    def _bind(self):

        if not self.__api_bound:

            for api_id in dtp_api_id.RSP_API_NAMES:
                self.dispatcher.bind(
                    f'{self.account_no}_{api_id}_rsp',
                    self._on_response)

#            for api_id in dtp_api_id.REQ_API_NAMES:
#                api_name = dtp_api_id.REQ_API_NAMES[api_id]
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
        if 'header' in mail:
            mail.body['message'] = mail.header.message

        if api_id == dtp_api_id.LOGIN_ACCOUNT_RESPONSE:
            self.on_login(mail)
        elif api_id == dtp_api_id.LOGOUT_ACCOUNT_RESPONSE:
            self.on_logout(mail)
        else:
            for ea in self._strategies:
                if ea.started and ea._check_owner(mail):
                    getattr(ea, dtp_api_id.RSP_API_NAMES[api_id])(mail)

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

    def cancel_order(self, order_exchange_id, exchange, **kw):
        """
        撤单
        """
        order_cancelation_req = dtp_api.OrderCancelationReq()
        order_cancelation_req.account_no = self.account_no
        order_cancelation_req.exchange = exchange
        order_cancelation_req.order_exchange_id = order_exchange_id
        return self._trade_api.cancel_order(order_cancelation_req)

    def query_orders(self, **kw):
        """
        查询订单
        """
        mail = attrdict()
        orders = self._trade_api.get_orders(account_no=self.account_no)
        orders = list(map(rename_order, orders))
        mail['body'] = orders
        return mail

    def query_trades(self, **kw):
        """
        查询成交
        """
        mail = attrdict()
        trades = self._trade_api.get_trades(account_no=self.account_no)
        trades = list(map(rename_trade, trades))
        mail['body'] = trades
        return mail

    def query_positions(self, **kw):
        """
        查询持仓
        """
        mail = attrdict()
        positions = self._trade_api.get_positions(account_no=self.account_no)
        positions = list(map(rename_position, positions))
        mail['body'] = positions
        return mail

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
