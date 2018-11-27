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

from fast_trader import zmq_context
from fast_trader.dtp import dtp_api_id
from fast_trader.dtp import api_pb2 as dtp_struct
from fast_trader.dtp import type_pb2 as dtp_type

from fast_trader.dtp import ext_api_pb2 as dtp_struct_
from fast_trader.dtp import ext_type_pb2 as dtp_type_

from fast_trader.utils import timeit, message2dict, load_config, Mail, _id_pool
from fast_trader.logging import setup_logging

from concurrent.futures import ProcessPoolExecutor
executor = ProcessPoolExecutor(2)

config = load_config()

REQUEST_TIMEOUT = 60


class Dispatcher(object):

    def __init__(self, **kw):

        self._handlers = {}

        # 收件箱
        self._inbox = Queue()
        # 发件箱
        self._outbox = Queue()

        self._market_queue = Queue()

        self.logger = logging.getLogger('fast_trader.dtp_trade.Dispatcher')

        self._running = False
        self.start()

    def start(self):

        if self._running:
            return

        self._running = True
        threading.Thread(target=self.process_inbox).start()
        threading.Thread(target=self.process_outbox).start()

    def process_inbox(self):

        while self._running:
            mail = self._inbox.get()
            self.dispatch(mail)

    def process_outbox_(self):

        while self._running:
            mail = self._outbox.get()
            self.dispatch(mail)

    def process_outbox(self):

        while self._running:
            try:
                mail = self._outbox.get(block=False)
                self.dispatch(mail)
            except queue.Empty:
                pass

            try:
                mail = self._market_queue.get(block=False)
                self.dispatch(mail)
            except queue.Empty:
                time.sleep(0.0001)

    def bind(self, handler_id, handler, override=False):
        if not override and handler_id in self._handlers:
            raise KeyError(
                'handler {} already exists!'.format(handler_id))
        self._handlers[handler_id] = handler

    def put(self, mail):

        handler_id = mail['handler_id']

        if mail.get('sync'):
            return self.dispatch(mail)

        if handler_id.endswith('_req'):
            self._inbox.put(mail)
        elif handler_id[0].isalpha():
            self._market_queue.put(mail)
        elif handler_id.endswith('_rsp'):
            self._outbox.put(mail)
        else:
            raise Exception('Invalid message: {}'.format(mail))

    def dispatch(self, mail):
        return self._handlers[mail['handler_id']](mail)


class DTPType(object):

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


class DTP(object):

    def __init__(self, dispatcher=None):

        self.dispatcher = dispatcher or Queue()

        self._account = config['account']
        self._ctx = zmq_context.CONTEXT

        # 同步查询通道
        self._sync_req_resp_channel = self._ctx.socket(zmq.REQ)
        self._sync_req_resp_channel.connect(config['sync_channel_port'])

        # 异步查询通道
        self._async_req_channel = self._ctx.socket(zmq.DEALER)
        self._async_req_channel.connect(config['async_channel_port'])

        # 异步查询响应通道
        self._async_resp_channel = self._ctx.socket(zmq.SUB)
        self._async_resp_channel.connect(config['rsp_channel_port'])
        self._async_resp_channel.subscribe('{}'.format(self._account))

        # 风控推送通道
        self._risk_report_channel = self._ctx.socket(zmq.SUB)
        self._risk_report_channel.connect(config['risk_channel_port'])
        self._async_resp_channel.subscribe('{}'.format(self._account))

        self.logger = logging.getLogger('fast_trader.dtp_trade.DTP')

        self.start()

    def start(self):

        self._running = True
        threading.Thread(target=self.handle_counter_response).start()
        threading.Thread(target=self.handle_compliance_report).start()

    def _populate_message(self, cmsg, attrs):

        for attr, value in attrs.items():
            name = attr
            if attr == 'account':
                name = 'account_no'

            if isinstance(value, list):
                repeated = getattr(cmsg, attr)
                for i in value:
                    item = repeated.add()
                    self._populate_message(item, i)
            elif isinstance(value, dict):
                nv = getattr(cmsg, attr)
                self._populate_message(nv, value)
            else:
                if hasattr(cmsg, name):
                    setattr(cmsg, name, value)

    def handle_sync_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = mail['request_id']
        header.api_id = mail['api_id']

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

            except zmq.ZMQError as e:
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
                    sync=sync
                )

                mail['header'] = message2dict(header)
                mail['body'] = message2dict(body)

                self.logger.info(mail)

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
        header.request_id = mail['request_id']
        header.api_id = mail['api_id']
        header.token = mail['token']

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

            self.logger.debug('topic: {}'.format(topic))
            header = dtp_struct.ReportHeader()
            header.ParseFromString(report_header)

            rsp_type = DTPType.get_proto_type(header.api_id)

            try:
                body = rsp_type()
                body.ParseFromString(report_body)
            except Exception:
                self.logger.warning('未知响应 api_id={}, {}'.format(
                    header.api_id, header.message))
                continue

            mail = Mail(
                api_id=header.api_id,
                api_type='rsp'
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
            self.logger.warning('风控消息 {}', header.api_id, header.message)

            body = dtp_struct.PlacedReport()
            body.ParseFromString(report_body)

            self.logger.info('{}, {}'.format(
                message2dict(header), message2dict(body)))


class Order(object):

    exchange = dtp_type.EXCHANGE_SH_A
    code = ''
    price = ''
    quantity = 0
    order_side = dtp_type.ORDER_SIDE_BUY
    order_type = dtp_type.ORDER_TYPE_LIMIT

    def __getitem__(self, key):
        return getattr(self, key)


class Trader(object):

    def __init__(self, dispatcher=None, broker=None, trader_id=0):

        self.dispatcher = dispatcher
        self.broker = broker

        self.trader_id = trader_id

        self._started = False
        self._request_id = 0
        self._order_original_id = 0

        self._account = ''
        self._token = ''
        self._logined = False

        self._position_results = []
        self._trade_results = []
        self._order_results = []

        self._strategies = []
        self._strategy_dict = OrderedDict()

        setup_logging()
        self.logger = logging.getLogger('fast_trader.dtp_trade.Trader')
        self.logger.info('初始化 process_id={}'.format(os.getpid()))

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

        dispatcher, broker = self.dispatcher, self.broker

        for api_id in dtp_api_id.RSP_API_NAMES:
            dispatcher.bind('{}_rsp'.format(api_id), self._on_response)

        for api_id in dtp_api_id.REQ_API_NAMES:
            api_name = dtp_api_id.REQ_API_NAMES[api_id]
            handler = getattr(broker, api_name)
            dispatcher.bind('{}_req'.format(api_id), handler)

    def _set_number(self, number, max_number=None):
        """
        预留接口

        如要同时启动多个trader实例, 为了保证请求/报单编号的唯一性，
        可以根据trader编号, 为其分配独立的取值范围
        """
        self._number = number
        if max_number:
            self._max_number = max_number

    def _generate_initial_id(self, strategy_id):
        """
        计算初始编号
        """

        # initial_id = self._id_ranges(self.trader_id, strategy_id)[0]
        id_range = _id_pool.get_range(self.trader_id, strategy_id)
        initial_id = id_range[0]
        self.logger.warning('初始请求编号 策略={} {}'.format(
            strategy_id, initial_id))

        setattr(self, '_order_id_{}'.format(strategy_id), initial_id)
        setattr(self, '_request_id_{}'.format(strategy_id), initial_id)

    def generate_request_id(self, number=1):
        """
        请求id，保证当日不重复
        """
        name = '{}_{}'.format('_request_id', number)

        request_id = getattr(self, name)
        setattr(self, name, request_id + 1)
        return str(request_id)

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
        self._generate_initial_id(strategy.strategy_id)

    def _is_assignee(self, strategy, mail):

        id_range = _id_pool.get_range(self.trader_id,
                                           strategy.strategy_id)

        if mail.header.request_id != '':

            if int(mail.header.request_id) in id_range:
                return True
            return False

        order_id = mail.body['order_original_id']

        if int(order_id) in id_range:
            return True
        return False

    def _on_response(self, mail):

        api_id = mail['api_id']

        mail.body['message'] = mail.header.message

        if api_id == dtp_api_id.LOGIN_ACCOUNT_RESPONSE:
            self.on_login(mail)
        elif api_id == dtp_api_id.LOGOUT_ACCOUNT_RESPONSE:
            self.on_logout(mail)
        else:
            for ea in self._strategies:
                if self._is_assignee(ea, mail):
                    getattr(ea, dtp_api_id.RSP_API_NAMES[api_id])(mail)

    @property
    def account_no(self):
        return self._account

    @property
    def logined(self):
        return self._logined

    def on_login(self, msg):
        try:
            self._token = msg['token']
        except Exception as e:
            self.logger.error('登录失败', exc_info=True)
            raise e

        self.logger.info('登入账户 {}, {}'.format(self.account_no, msg))

    def on_logout(self, mail):
        self.logger.info('登出账户 {}'.format(self.account_no))

    def login(self, account, password, sync=True, **kw):

        self._account = account
        ret = self.login_account(account=account,
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
            account=self._account,
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

    def send_order(self, request_id, order_original_id, exchange,
                   code, price, quantity, order_side,
                   order_type=dtp_type.ORDER_TYPE_LIMIT):
        """
        报单委托
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.PLACE_ORDER,
            account=self._account,
            token=self._token,
            request_id=request_id,
            order_original_id=order_original_id,
            exchange=exchange,
            code=code,
            price=price,
            quantity=quantity,
            order_side=order_side,
            order_type=order_type
        )
        self.dispatcher.put(mail)

        self.logger.info('报单委托 {}'.format(mail))

    def place_order_batch(self, request_id, orders):
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

    def cancel_order(self, **kw):
        """
        撤单
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.CANCEL_ORDER,
            account=self._account,
            token=self._token,
            **kw
        )
        self.dispatcher.put(mail)

    def query_orders(self, **kw):
        """
        查询订单
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_ORDERS_REQUEST,
            account=self._account,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)

    def query_trades(self, **kw):
        """
        查询成交
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_FILLS_REQUEST,
            account=self._account,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)

    def query_positions(self, **kw):
        """
        查询持仓
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_POSITION_REQUEST,
            account=self._account,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)

    def query_capital(self, **kw):
        """
        查询账户资金
        """
        mail = Mail(
            api_type='req',
            api_id=dtp_api_id.QUERY_CAPITAL_REQUEST,
            account=self._account,
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
            account=self._account,
            token=self._token,
            **kw
        )
        return self.dispatcher.put(mail)


class PositionDetail(object):
    """
    交易标的持仓状态
    """

    # 代码
    code = ''
    # 交易所
    exchange = ''
    # 当前持仓
    position = 0
    # 昨日最终持仓
    yd_last_position = 0
    # 昨日持仓均价
    yd_avg_price = 0

    def __init__(self):
        pass


class AccountDetail(object):
    """
    账户详情
    """
    # 账号
    account_no = ''
    # 当前权益
    current_capital = 0.
    # 现金余额
    balance = 0.


if __name__ == '__main__':

    trader = Trader()
    trader.start()
    trader.login(**config)
