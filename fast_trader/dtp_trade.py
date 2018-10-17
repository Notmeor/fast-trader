# -*- coding: utf-8 -*-

import zmq
import time
import random
import threading
from queue import Queue

from fast_trader.dtp import dtp_api_id as dtp_api_id
from fast_trader.dtp import api_pb2 as dtp_struct
from fast_trader.dtp import type_pb2 as dtp_type

SYNC_CHANNEL_PORT = "tcp://192.168.221.52:9203"
SYNC_CHANNEL_PORT = "tcp://192.168.221.52:9003"

LOGIN_ACCOUNT_REQUEST = 10001001
LOGIN_ACCOUNT_RESPONSE = 11001001
LOGOUT_ACCOUNT_REQUEST = 10001002
LOGOUT_ACCOUNT_RESPONSE = 11001002
QUERY_ORDERS_REQUEST = 10003001
QUERY_ORDERS_RESPONSE = 11003001
QUERY_FILLS_REQUEST = 10003002
QUERY_FILLS_RESPONSE = 11003002
QUERY_CAPITAL_REQUEST = 10003003
QUERY_CAPITAL_RESPONSE = 11003003
QUERY_POSITION_REQUEST = 10003004
QUERY_POSITION_RESPONSE = 11003004
QUERY_RATION_REQUEST = 10005001
QUERY_RATION_RESPONSE = 11005001

PLACE_ORDER = 10002001
CANCEL_ORDER = 10002002
PLACE_BATCH_ORDER = 10002003

PLACE_REPORT = 20001001
FILL_REPORT = 20001002
CANCEL_REPORT = 20001003
CANCEL_REPORT_SUB = 11002002

config = {
    'sync_channel_port': 'tcp://192.168.221.52:9003',
    'async_channel_port': 'tcp://192.168.221.91:9101',
    'resp_channel_port': 'tcp://192.168.221.52:9002',
    'account': '021000062436',
    'password': '111111',
}


def generate_request_id():
    return str(random.randrange(11000000, 11900000))


def generate_original_id():
    return str(random.randrange(61000000, 61900000))


class Payload(object):
    def __init__(self, header, body):
        self.header = header
        self.body = body


class Dispatcher(object):
    """
    消息中转
    """
    def __init__(self, **kw):

        self._handlers = {}

        # 收件箱
        self._inbox = Queue()
        # 发件箱
        self._outbox = Queue()

        self._running = False
        self._run()

    def _run(self):

        self._running = True
        threading.Thread(target=self.process_inbox).start()
        threading.Thread(target=self.process_outbox).start()

    def process_inbox(self):

        while self._running:
            mail = self._inbox.get()
            self.dispatch(mail)

    def process_outbox(self):

        while self._running:
            mail = self._outbox.get()
            self.dispatch(mail)

    def bind(self, handler_id, handler, override=False):
        if not override and handler_id in self._handlers:
            raise KeyError(
                'handler {} already exists!')
        self._handlers[handler_id] = handler

    def put(self, mail):
        handler_id = mail['handler_id']
        if handler_id.endswith('_req'):
            self._inbox.put(mail)
        elif handler_id.endswith('_resp'):
            self._outbox.put(mail)
        else:
            print('Invalid message: {}'.format(mail))

    def dispatch(self, mail):
        self._handlers[mail['handler_id']](mail)


class DTP(object):

    def __init__(self, dispatcher=None):

        self.dispatcher = dispatcher or Queue()

        self._ctx = zmq.Context()

        # 同步查询通道
        self._sync_req_resp_channel = self._ctx.socket(zmq.REQ)
        self._sync_req_resp_channel.connect(config['sync_channel_port'])

        # 异步查询通道
        self._async_req_channel = self._ctx.socket(zmq.DEALER)
        self._async_req_channel.connect(config['async_channel_port'])

        # 异步查询响应通道
        self._async_resp_channel = self._ctx.socket(zmq.SUB)
        self._async_resp_channel.connect(config['resp_channel_port'])
        self._async_resp_channel.subscribe('')

        # 风控推送通道
        self._risk_report_channel = self._ctx.socket(zmq.SUB)

        self.start()

    def start(self):

        self._running = True
        threading.Thread(target=self.handle_counter_response).start()

    def handle_login_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.api_id = mail['api_id']

        body = dtp_struct.LoginAccountRequest()
        body.account_no = mail['account']
        body.password = mail['password']

        payload = Payload(header, body)

        self._handle_sync_request(payload)

        self.handle_login_response()

    def handle_login_response(self):
        self._handle_sync_response(dtp_struct.LoginAccountResponse)

    def handle_logout_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.api_id = mail['api_id']
        header.token = mail['token']

        body = dtp_struct.LoginAccountRequest()
        body.account_no = mail['account']
        # body.password = mail['password']

        payload = Payload(header, body)

        self._handle_sync_request(payload)

        self.handle_logout_response()

    def handle_logout_response(self):
        self._handle_sync_response(dtp_struct.LogoutAccountResponse)

    def handle_send_order_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.api_id = dtp_api_id.PLACE_ORDER
        header.token = mail['token']

        body = dtp_struct.PlaceOrder()
        body.account_no = mail['account']
        body.order_original_id = generate_original_id()
        body.exchange = mail['exchange']
        body.code = mail['code']
        body.price = mail['price']
        body.quantity = mail['quantity']
        body.order_side = mail['order_side']
        body.order_type = mail['order_type']

        order_payload = Payload(header, body)

        self._handle_async_request(order_payload)

    def handle_batch_order_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.api_id = dtp_api_id.PLACE_BATCH_ORDER
        header.token = mail['token']

        body = dtp_struct.PlaceBatchOrder()
        body.account_no = mail['account']

        for order in mail['orders']:
            item = body.order_list.add()
            item.order_original_id = generate_original_id()
            item.exchange = order['exchange']
            item.code = order['code']
            item.price = order['price']
            item.quantity = order['quantity']
            item.order_side = order['order_side']
            item.order_type = order['order_type']

        order_payload = Payload(header, body)

        self._handle_async_request(order_payload)

    def handle_order_cancelation_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.api_id = dtp_api_id.CANCEL_ORDER
        header.token = mail['token']

        body = dtp_struct.CancelOrder()
        body.account_no = mail['account']
        body.exchange = mail['exchange']
        body.order_exchange_id = mail['order_exchange_id']

        order_payload = Payload(header, body)

        self._handle_async_request(order_payload)

    def handle_query_order_request(self, mail):
        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.api_id = dtp_api_id.QUERY_ORDERS_REQUEST
        header.token = mail['token']

        body = dtp_struct.QueryOrdersRequest()
        body.account_no = mail['account']

        payload = Payload(header, body)

        self._handle_sync_request(payload)

        self.handle_query_order_response()

    def handle_query_order_response(self):
        self._handle_sync_response(dtp_struct.QueryOrdersResponse)

    def handle_query_trade_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.api_id = dtp_api_id.QUERY_FILLS_REQUEST
        header.token = mail['token']

        body = dtp_struct.QueryFillsRequest()
        body.account_no = mail['account']

        payload = Payload(header, body)

        self._handle_sync_request(payload)

        self.handle_query_trade_response()

    def handle_query_trade_response(self):
        self._handle_sync_response(dtp_struct.QueryFillsResponse)

    def handle_query_position_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.api_id = dtp_api_id.QUERY_POSITION_REQUEST
        header.token = mail['token']

        body = dtp_struct.QueryPositionRequest()
        body.account_no = mail['account']
        # body.exchange = mail['exchange']
        payload = Payload(header, body)

        self._handle_sync_request(payload)

        self.handle_query_position_response()

    def handle_query_position_response(self):
        self._handle_sync_response(dtp_struct.QueryPositionResponse)

    def handle_query_capital_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.api_id = dtp_api_id.QUERY_CAPITAL_REQUEST
        header.request_id = generate_request_id()
        print('token:', mail['token'])
        header.token = mail['token']

        body = dtp_struct.QueryCapitalRequest()
        body.account_no = mail['account']
        payload = Payload(header, body)

        print(payload.header, payload.body)

        self._handle_sync_request(payload)

        self.handle_query_capital_response()

    def handle_query_capital_response(self):
        self._handle_sync_response(dtp_struct.QueryCapitalResponse)

    def handle_query_ration_request(self, mail):

        header = dtp_struct.RequestHeader()
        header.api_id = dtp_api_id.QUERY_RATION_REQUEST
        header.request_id = generate_request_id()
        header.token = mail['token']

        body = dtp_struct.QueryRationRequest()
        body.account_no = mail['account']
        payload = Payload(header, body)

        print(payload.header, payload.body)

        self._handle_sync_request(payload)

        self.handle_query_ration_response()

    def handle_query_ration_response(self):
        self._handle_sync_response(dtp_struct.QueryRationResponse)

    def handle_counter_response(self):

        sock = self._async_resp_channel

        while self._running:

            topic = sock.recv()
            report_header = sock.recv()
            report_body = sock.recv()

            header = dtp_struct.ReportHeader()
            header.ParseFromString(report_header)
            print('handler_id:', header.api_id)
            if(header.api_id == dtp_api_id.PLACE_REPORT):
                body = dtp_struct.PlacedReport()
                body.ParseFromString(report_body)

            elif(header.api_id == dtp_api_id.FILL_REPORT):
                body = dtp_struct.FillReport()
                body.ParseFromString(report_body)

            elif(header.api_id == dtp_api_id.CANCEL_REPORT):
                body = dtp_struct.CancellationReport()
                body.ParseFromString(report_body)

            else:
                print('unknown resp:', header, header.message)
                continue

            handler_id = '{}_resp'.format(header.api_id)

            self.dispatcher.put({
                'handler_id': handler_id,
                'content': Payload(header, body)
            })

    def _handle_sync_request(self, payload):
        try:
            print(payload.header, payload.body)
            self._sync_req_resp_channel.send(
                payload.header.SerializeToString(), zmq.SNDMORE)

            self._sync_req_resp_channel.send(
                payload.body.SerializeToString())
        except zmq.ZMQError as e:
            print('e:', e)
            print('正在查询中...')

    def _handle_sync_response(self, resp_type):

        waited_time = 0

        while waited_time < 5:

            try:
                _header = self._sync_req_resp_channel.recv(flags=zmq.NOBLOCK)
                _body = self._sync_req_resp_channel.recv(flags=zmq.NOBLOCK)
            except zmq.ZMQError as e:

                time.sleep(0.1)
                waited_time += 0.1
            else:

                response_header = dtp_struct.ResponseHeader()
                response_header.ParseFromString(_header)

                response_body = resp_type()
                response_body.ParseFromString(_body)
                payload = Payload(response_header, response_body)

                self.dispatcher.put({
                    'handler_id': '{}_resp'.format(response_header.api_id),
                    'content': payload
                })
                return

        print('{} 查询超时'.format(resp_type))

    def _handle_async_request(self, payload):

        self._async_req_channel.send(
            payload.header.SerializeToString(), zmq.SNDMORE)

        self._async_req_channel.send(
            payload.body.SerializeToString())


class Mail(object):

    def __init__(self, api_id, api_type, **kw):

        if 'handler_id' not in kw:
            kw['handler_id'] = '{}_{}'.format(api_id, api_type)

        kw.update({
            'api_id': api_id,
            'api_type': api_type
        })

        self._kw = kw

    def __getitem__(self, key):
        return self._kw[key]


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

    def __init__(self):

        self._account = ''
        self._token = ''

    def start(self):

        self.dispatcher = dispatcher = Dispatcher()
        self.broker = broker = DTP(dispatcher)

        dispatcher.bind(
            '{}_resp'.format(LOGIN_ACCOUNT_RESPONSE),
            self._on_login)

        dispatcher.bind(
            '{}_resp'.format(LOGOUT_ACCOUNT_RESPONSE),
            self._on_logout)

        dispatcher.bind(
            '{}_resp'.format(PLACE_REPORT),
            self._on_order)

        dispatcher.bind(
            '{}_resp'.format(FILL_REPORT),
            self._on_trade)

        dispatcher.bind(
            '{}_resp'.format(QUERY_ORDERS_RESPONSE),
            self._on_order_query)

        dispatcher.bind(
            '{}_resp'.format(QUERY_FILLS_RESPONSE),
            self._on_trade_query)

        dispatcher.bind(
            '{}_resp'.format(QUERY_POSITION_RESPONSE),
            self._on_position_query)

        dispatcher.bind(
            '{}_resp'.format(QUERY_CAPITAL_RESPONSE),
            self._on_capital_query)

        dispatcher.bind(
            '{}_resp'.format(QUERY_RATION_RESPONSE),
            self._on_ration_query)

        dispatcher.bind(
            '{}_resp'.format(CANCEL_REPORT),
            self._on_order_cancelation)

        dispatcher.bind(
            '{}_req'.format(LOGIN_ACCOUNT_REQUEST),
            broker.handle_login_request)

        dispatcher.bind(
            '{}_req'.format(LOGOUT_ACCOUNT_REQUEST),
            broker.handle_logout_request)

        dispatcher.bind(
            '{}_req'.format(PLACE_ORDER),
            broker.handle_send_order_request)

        dispatcher.bind(
            '{}_req'.format(PLACE_BATCH_ORDER),
            broker.handle_batch_order_request)

        dispatcher.bind(
            '{}_req'.format(QUERY_ORDERS_REQUEST),
            broker.handle_query_order_request)

        dispatcher.bind(
            '{}_req'.format(QUERY_FILLS_REQUEST),
            broker.handle_query_trade_request)

        dispatcher.bind(
            '{}_req'.format(QUERY_POSITION_REQUEST),
            broker.handle_query_position_request)

        dispatcher.bind(
            '{}_req'.format(QUERY_CAPITAL_REQUEST),
            broker.handle_query_capital_request)

        dispatcher.bind(
            '{}_req'.format(QUERY_RATION_REQUEST),
            broker.handle_query_ration_request)

        dispatcher.bind(
            '{}_req'.format(CANCEL_ORDER),
            broker.handle_order_cancelation_request)

    def _on_login(self, mail):
        response = mail['content']
        self._token = response.body.token
        print(response.header)
        print(response.body)

    def _on_logout(self, mail):
        response = mail['content']
        print(response.header)
        print(response.body)

    def _on_order(self, mail):
        print('----- 报单回报 -----')
        response = mail['content']
        print(response.header)
        print(response.header.message)
        print(response.body)

    def _on_trade(self, mail):
        print('----- 成交回报 -----')
        response = mail['content']
        print(response.header)
        print(response.body)

    def _on_order_query(self, mail):
        print('----- 订单查询回报 -----')
        response = mail['content']
        print(response.header)
        print(response.body)

    def _on_trade_query(self, mail):
        print('----- 成交查询回报 -----')
        response = mail['content']
        print(response.header)
        print(response.body)

    def _on_position_query(self, mail):
        print('----- 持仓查询回报 -----')
        response = mail['content']
        print(response.header)
        print(response.body)

    def _on_capital_query(self, mail):
        print('----- 资金查询回报 -----')
        response = mail['content']
        print(response.header)
        print(response.body)

    def _on_ration_query(self, mail):
        print('----- 配售权益查询回报 -----')
        response = mail['content']
        print(response.header)
        print(response.body)

    def _on_order_cancelation_submission(self, mail):
        print('----- 撤销申请接收回报 -----')
        response = mail['content']
        print(response.header)
        print(response.body)

    def _on_order_cancelation(self, mail):
        print('----- 报单撤销回报 -----')
        response = mail['content']
        print(response.header)
        print(response.body)

    def login(self, account, password, **kw):
        self._account = account
        mail = Mail(
            api_type='req',
            api_id=LOGIN_ACCOUNT_REQUEST,
            account=account,
            password=password
        )
        self.dispatcher.put(mail)

        timeout = 0
        while True:
            if self._token != '':
                break
            time.sleep(0.5)
            timeout += 0.5
            if timeout > 5:
                raise Exception('登录超时!')

    def logout(self, **kw):
        mail = Mail(
            api_type='req',
            api_id=LOGOUT_ACCOUNT_REQUEST,
            account=self._account,
            token=self._token
        )
        self.dispatcher.put(mail)

    def send_order(self, exchange, code, price, quantity,
                   order_side, order_type=dtp_type.ORDER_TYPE_LIMIT):
        """
        下单
        """
        mail = Mail(
            api_type='req',
            api_id=PLACE_ORDER,
            account=self._account,
            token=self._token,
            exchange=exchange,
            code=code,
            price=price,
            quantity=quantity,
            order_side=order_side,
            order_type=order_type
        )
        self.dispatcher.put(mail)

    def place_order(self, order):

        mail = Mail(
            api_type='req',
            api_id=PLACE_ORDER,
            account=self._account,
            token=self._token,
            exchange=order.exchange,
            code=order.code,
            price=order.price,
            quantity=order.quantity,
            order_side=order.order_side,
            order_type=order.order_type
        )
        self.dispatcher.put(mail)

    def place_order_batch(self, orders):
        """
        批量下单
        """
        mail = Mail(
            api_type='req',
            api_id=PLACE_BATCH_ORDER,
            account=self._account,
            token=self._token,
            orders=orders
        )
        self.dispatcher.put(mail)

    def send_order_batch(self, batch):
        """
        批量下单
        """
        mail = Mail(
            api_type='req',
            api_id=PLACE_ORDER,
            account=self._account,
            token=self._token,
            order_list=batch
        )
        self.dispatcher.put(mail)

    def cancel_order(self, exchange, order_exchange_id, **kw):
        """
        撤单
        """
        mail = Mail(
            api_type='req',
            api_id=CANCEL_ORDER,
            account=self._account,
            token=self._token,
            exchange=exchange,
            order_exchange_id=order_exchange_id
        )
        self.dispatcher.put(mail)

    def query_orders(self, **kw):
        """
        查询订单
        """
        mail = Mail(
            api_type='req',
            api_id=QUERY_ORDERS_REQUEST,
            account=self._account,
            token=self._token
        )
        self.dispatcher.put(mail)

    def query_trades(self, **kw):
        """
        查询成交
        """
        mail = Mail(
            api_type='req',
            api_id=QUERY_FILLS_REQUEST,
            account=self._account,
            token=self._token
        )
        self.dispatcher.put(mail)

    def query_position(self, **kw):
        """
        查询持仓
        """
        mail = Mail(
            api_type='req',
            api_id=QUERY_POSITION_REQUEST,
            account=self._account,
            token=self._token,
            exchange=kw['exchange'],
            code=kw['code'],
            pagination=kw['pagination']
        )
        self.dispatcher.put(mail)

    def query_capital(self, **kw):
        """
        查询账户资金
        """
        mail = Mail(
            api_type='req',
            api_id=QUERY_CAPITAL_REQUEST,
            account=self._account,
            token=self._token,
            exchange=kw['exchange'],
            code=kw['code'],
            pagination=kw['pagination']
        )
        self.dispatcher.put(mail)

    def query_ration(self, **kw):
        """
        查询配售权益
        """
        mail = Mail(
            api_type='req',
            api_id=QUERY_RATION_REQUEST,
            account=self._account,
            token=self._token
        )
        self.dispatcher.put(mail)

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

    def on_account(self, account):
        """
        账户查询回报
        """
        pass

    def on_compliance_report(self, report):
        """
        风控回报
        """
        pass


def test_send_order(trader):
    trader.send_order(
        exchange=dtp_type.EXCHANGE_SH_A,
        code='601398',
        price='1.9',
        quantity=500,
        order_side=dtp_type.ORDER_SIDE_BUY
    )

if __name__ == '__main__':

    login_mail = {
       'api_id':  LOGIN_ACCOUNT_REQUEST,
       'account': config['account'],
       'password': config['password']
    }

    trader = Trader()
    trader.start()
    trader.login(**login_mail)

    trader.query_capital(
        account=config['account'],
        exchange=dtp_type.EXCHANGE_SH_A,
        code='',
        pagination=''
    )
#
    trader.query_position(
        account=config['account'],
        exchange=dtp_type.EXCHANGE_SH_A,
        code='',
        pagination=''
    )

    trader.query_trades(
        account=config['account']
    )

    order_0 = Order()
    order_1 = Order()
    order_0.price = '1.96'
    order_0.quantity = 2500
    order_0.code = '601398'

    order_1.price = '2.96'
    order_1.code = '601399'
    order_1.quantity = 3000