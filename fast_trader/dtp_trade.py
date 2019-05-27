# -*- coding: utf-8 -*-

import os
import time
import datetime

import threading
import logging
import zmq
import queue
from queue import Queue
from collections import OrderedDict
import uuid

from fast_trader import zmq_context
from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import api_pb2 as dtp_struct
from fast_trader.dtp import type_pb2 as dtp_type

from fast_trader.dtp import ext_api_pb2 as dtp_struct_
from fast_trader.dtp import ext_type_pb2 as dtp_type_

from fast_trader.id_pool import _id_pool
from fast_trader.utils import timeit, attrdict, message2dict, Mail
from fast_trader.settings import settings, setup_logging
from fast_trader.rest_api import might_use_rest_api

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
            self.dispatch(mail)

    def process_rsp(self):

        # TODO: poll queues or zmq socks
        while self._running:
            try:
                mail = self._rsp_queue.get(block=False)
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


class DTP:

    def __init__(self, dispatcher=None):

        self.dispatcher = dispatcher or Queue()

        self.__settings = settings.copy()

        self._ctx = zmq_context.CONTEXT

        # 同步查询通道
        self._sync_req_resp_channel = self._ctx.socket(zmq.REQ)
        self._sync_req_resp_channel.connect(settings['sync_channel_port'])

        # 异步查询通道
        self._async_req_channel = self._ctx.socket(zmq.DEALER)
        self._async_req_channel.connect(settings['async_channel_port'])

        # 异步查询响应通道
        self._async_resp_channel = self._ctx.socket(zmq.SUB)
        self._async_resp_channel.connect(settings['rsp_channel_port'])
        self._async_resp_channel.subscribe('')
        # self._async_resp_channel.subscribe('{}'.format(self._account_no))

        ## 风控推送通道
        #self._risk_report_channel = self._ctx.socket(zmq.SUB)
        #self._risk_report_channel.connect(settings['risk_channel_port'])
        #self._risk_report_channel.subscribe('{}'.format(self._account_no))

        self.logger = logging.getLogger('dtp')

        self.start()

    def start(self):

        self._running = True
        threading.Thread(target=self.handle_counter_response).start()
        # 柜台回报似乎已经包含了合规消息
        # threading.Thread(target=self.handle_compliance_report).start()

    def _populate_message(self, cmsg, attrs):

        for name, value in attrs.items():

            if isinstance(value, list):
                repeated = getattr(cmsg, name)
                for i in value:
                    item = repeated.add()
                    self._populate_message(item, i)
            elif isinstance(value, dict):
                nv = getattr(cmsg, name)
                self._populate_message(nv, value)
            else:
                if hasattr(cmsg, name):
                    setattr(cmsg, name, value)

    def handle_sync_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = mail['request_id']
        header.api_id = mail['api_id']
        header.account_no = mail['account_no']
        header.ip = self.__settings['ip']
        header.mac = self.__settings['mac']
        header.harddisk = self.__settings['harddisk']

        token = mail.get('token')
        if token:
            header.token = mail['token']

        req_type = DTPType.get_proto_type(mail['api_id'])

        body = req_type()

        self._populate_message(body, mail)

        mail.pop('password', None)
        self.logger.info(mail)

        try:
            self._sync_req_resp_channel.send(
                header.SerializeToString(), zmq.SNDMORE)
            self._sync_req_resp_channel.send(
                body.SerializeToString())
        except zmq.ZMQError:
            self.logger.error('查询响应中...', exc_info=True)

        return self.handle_sync_response(
            api_id=mail['api_id'], sync=mail['sync'])

    def handle_sync_response(self, api_id, sync=False):

        waited_time = 0

        while waited_time < REQUEST_TIMEOUT:

            try:
                report_header = self._sync_req_resp_channel.recv(
                    flags=zmq.NOBLOCK)

                report_body = self._sync_req_resp_channel.recv(
                    flags=zmq.NOBLOCK)

            except zmq.ZMQError:
                time.sleep(0.0001)
                waited_time += 0.0001

            else:
                header = dtp_struct.ResponseHeader()
                header.ParseFromString(report_header)

                api_id = header.api_id
                rsp_type = DTPType.get_proto_type(api_id)

                body = rsp_type()
                body.ParseFromString(report_body)

                mail = Mail(
                    api_id=api_id,
                    api_type='rsp',
                    handler_id=f'{body.account_no}_{api_id}_rsp',
                    sync=sync
                )

                mail['header'] = message2dict(header)
                mail['body'] = message2dict(body)

                self.logger.info(mail.header)

                if sync:
                    return mail

                return self.dispatcher.put(mail)

        mail = Mail(
            api_id=api_id,
            api_type='rsp',
            sync=sync,
            ret_code=-1,
            err_message='请求超时'
        )
        self.logger.error('请求超时 api_id={}'.format(api_id))
        if sync:
            return mail
        return self.dispatcher.put(mail)

    def handle_async_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.token = mail['token']
        header.request_id = mail['request_id']
        header.api_id = mail['api_id']
        header.account_no = mail['account_no']
        header.ip = self.__settings['ip']
        header.mac = self.__settings['mac']
        header.harddisk = self.__settings['harddisk']

        req_type = DTPType.get_proto_type(header.api_id)
        body = req_type()

        self._populate_message(body, mail)

        self.logger.info(mail)

        self._async_req_channel.send(
            header.SerializeToString(), zmq.SNDMORE)

        self._async_req_channel.send(
            body.SerializeToString())

    def handle_counter_response(self):

        sock = self._async_resp_channel

        while self._running:

            topic = sock.recv()
            report_header = sock.recv()
            report_body = sock.recv()

            header = dtp_struct.ReportHeader()
            header.ParseFromString(report_header)

            api_id = header.api_id
            rsp_type = DTPType.get_proto_type(api_id)

            try:
                body = rsp_type()
                body.ParseFromString(report_body)
            except Exception:
                self.logger.warning('未知响应 api_id={}, {}'.format(
                    header.api_id, header.message))
                continue

            mail = Mail(
                api_id=api_id,
                api_type='rsp',
                handler_id=f'{body.account_no}_{api_id}_rsp',
            )

            mail['header'] = message2dict(header)
            mail['body'] = message2dict(body)

            self.logger.info(mail)

            self.dispatcher.put(mail)

    def handle_compliance_report(self):
        """
        风控消息推送
        """
        sock = self._risk_report_channel

        while self._running:

            topic = sock.recv()
            report_header = sock.recv()
            report_body = sock.recv()

            header = dtp_struct.ReportHeader()
            header.ParseFromString(report_header)

            body = dtp_struct.PlacedReport()
            body.ParseFromString(report_body)

            self.logger.info('合规风控 {}, {}'.format(
                message2dict(header), message2dict(body)))


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

    def __init__(self, dispatcher=None, broker=None, trader_id=0):

        self.dispatcher = dispatcher
        self.broker = broker

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

    def start(self):

        if self._started:
            return

        if self.dispatcher is None:
            self.dispatcher = Dispatcher()

        if self.broker is None:
            self.broker = DTP(self.dispatcher)

        self._bind()

        self._started = True

    def _bind(self):

        if not self.__api_bound:

            dispatcher, broker = self.dispatcher, self.broker

            for api_id in dtp_api_id.RSP_API_NAMES:
                dispatcher.bind(f'{self.account_no}_{api_id}_rsp',
                                self._on_response)

            for api_id in dtp_api_id.REQ_API_NAMES:
                api_name = dtp_api_id.REQ_API_NAMES[api_id]
                handler = getattr(broker, api_name)
                dispatcher.bind(f'{api_id}_req', handler)

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
                if ea._check_owner(mail):
                    getattr(ea, dtp_api_id.RSP_API_NAMES[api_id])(mail)

    @property
    def account_no(self):
        return self._account_no

    @property
    def logined(self):
        return self._logined

    def on_login(self, msg):
        try:
            self._token = msg['token']
        except Exception as e:
            self.logger.warning('登录失败', exc_info=True)
            raise e

        self.logger.info('登入账户 {}, {}'.format(self.account_no, msg))

    def on_logout(self, mail):
        self.logger.info('登出账户 {}'.format(self.account_no))

    @might_use_rest_api(might=settings['use_rest_api'],
                        api_name='restapi_login')
    def login(self, account_no, password, sync=True, **kw):

        self._account_no = account_no

        self.start()

        ret = self.login_account(account_no=account_no,
                                 password=password,
                                 sync=True, **kw)

        if ret['ret_code'] != 0:
            raise Exception(ret['err_message'])

        if sync:

            login_msg = ret['header']['message']

            if ret['header']['code'] == dtp_type.RESPONSE_CODE_OK:
                self._logined = True
                self._token = ret['body']['token']

                self.logger.info(
                    '登录成功 <{}> {}'.format(self.account_no, login_msg))

            else:
                self.logger.warning(
                    '登录失败 <{}> {}'.format(self.account_no, login_msg))

            return ret

    def logout(self, **kw):
        """
        登出暂为空操作
        """
        pass

    def logout_account(self, **kw):
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.LOGOUT_ACCOUNT_REQUEST,
            request_id=kw['request_id'],
            account_no=self._account_no,
            token=self._token
        )
        self.dispatcher.put(mail)

    def login_account(self, **kw):
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.LOGIN_ACCOUNT_REQUEST,
            **kw
        )
        return self.dispatcher.put(mail)

    @might_use_rest_api(might=settings['use_rest_api'],
                        api_name='restapi_place_order')
    def place_order(self, request_id, order_original_id, exchange,
                    code, price, quantity, order_side,
                    order_type=dtp_type.ORDER_TYPE_LIMIT):
        """
        报单委托
        """
        # 报价转为str类型
        price_ = price

        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.PLACE_ORDER,
            account_no=self._account_no,
            token=self._token,
            request_id=request_id,
            order_original_id=order_original_id,
            exchange=exchange,
            code=code,
            price=price_,
            quantity=quantity,
            order_side=order_side,
            order_type=order_type
        )
        self.dispatcher.put(mail)

        self.logger.info('报单委托 {}'.format(mail))

    @might_use_rest_api(might=settings['use_rest_api'],
                        api_name='restapi_place_batch_order')
    def place_batch_order(self, request_id, orders):
        """
        批量下单
        """

        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.PLACE_BATCH_ORDER,
            request_id=request_id,
            token=self._token,
            order_list=orders
        )
        self.dispatcher.put(mail)
        self.logger.info('批量买入委托 {}'.format(mail))

    @might_use_rest_api(might=settings['use_rest_api'],
                        api_name='restapi_cancel_order')
    def cancel_order(self, **kw):
        """
        撤单
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.CANCEL_ORDER,
            account_no=self._account_no,
            token=self._token,
            **kw
        )
        self.dispatcher.put(mail)

    @might_use_rest_api(might=settings['use_rest_api'],
                        api_name='restapi_query_orders')
    def query_orders(self, **kw):
        """
        查询订单
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_ORDERS_REQUEST,
            account_no=self._account_no,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)

    @might_use_rest_api(might=settings['use_rest_api'],
                        api_name='restapi_query_fills')
    def query_trades(self, **kw):
        """
        查询成交
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_FILLS_REQUEST,
            account_no=self._account_no,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)

    @might_use_rest_api(might=settings['use_rest_api'],
                        api_name='restapi_query_positions')
    def query_positions(self, **kw):
        """
        查询持仓
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_POSITION_REQUEST,
            account_no=self._account_no,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)

    @might_use_rest_api(might=settings['use_rest_api'],
                        api_name='restapi_query_capital')
    def query_capital(self, **kw):
        """
        查询账户资金
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_CAPITAL_REQUEST,
            account_no=self._account_no,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)

    def query_ration(self, **kw):
        """
        查询配售权益
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_RATION_REQUEST,
            account_no=self._account_no,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)


if __name__ == '__main__':

    trader = Trader()
    trader.start()
    trader.login(
        account_no=settings['account_no'],
        password=settings['password'],
    )
