# -*- coding: utf-8 -*-

import zmq
import time
import random
import threading
from queue import Queue

import dtp.dtp_api_id as dtp_api_id
import dtp.api_pb2 as dtp_struct
import dtp.type_pb2 as dtp_type

SYNC_CHANNEL_PORT = "tcp://192.168.221.52:9203"
SYNC_CHANNEL_PORT = "tcp://192.168.221.52:9003"

LOGIN_ACCOUNT_REQUEST  = 10001001
LOGIN_ACCOUNT_RESPONSE = 11001001
LOGOUT_ACCOUNT_REQUEST  = 10001002
LOGOUT_ACCOUNT_RESPONSE = 11001002
QUERY_ORDERS_REQUEST  = 10003001
QUERY_ORDERS_RESPONSE = 11003001
QUERY_FILLS_REQUEST  = 10003002
QUERY_FILLS_RESPONSE = 11003002
QUERY_CAPITAL_REQUEST  = 10003003
QUERY_CAPITAL_RESPONSE = 11003003
QUERY_POSITION_REQUEST  = 10003004
QUERY_POSITION_RESPONSE = 11003004
QUERY_RATION_REQUEST  = 10005001
QUERY_RATION_RESPONSE = 11005001

PLACE_ORDER = 10002001
CANCEL_ORDER = 10002002
PLACE_BATCH_ORDER = 10002003

PLACE_REPORT = 20001001
FILL_REPORT = 20001002
CANCEL_REPORT = 20001003

config = {
    'sync_channel_port': 'tcp://192.168.221.52:9003',
    'async_channel_port': 'tcp://192.168.221.91:9101',
    'resp_channel_port': 'tcp://192.168.221.52:9002',
    'account': '021000062436',
    'password': '111111',
    }

def generate_request_id():
    return str(random.randrange(11000000,11900000))

def generate_original_id():
    return str(random.randrange(61000000,61900000))

class Payload(object):
    def __init__(self, header, body):
        self.header = header
        self.body = body


def query_capital(token):
    header = dtp_struct.RequestHeader()
    header.request_id = generate_request_id()
    header.token = token
    body = dtp_struct.QueryCapitalRequest()
    body.account_no = "021000062436"
    payload = Payload(header, body)

    response_payload = dtp_sync_channel.query_capital(payload)
    print("Query Capital ResponseHeader:")
    print(response_payload.header)
    print("Query Capital ResponseBody:")
    print(response_payload.body)
    
    
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
    
    def bind(self, handler_id, handler):
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
        
        self._token = ''
        
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
        
        body = dtp_struct.LoginAccountRequest()
        body.account_no = mail['account']
        body.password = mail['password']
        
        payload = Payload(header, body)
        payload.header.api_id = mail['api_id']
        
        self._handle_sync_request(payload)
        
        self.handle_login_response()

    def handle_login_response(self):
        self._handle_sync_response(dtp_struct.LoginAccountResponse)

    def handle_logout_request(self, mail):
        
        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        
        body = dtp_struct.LoginAccountRequest()
        body.account_no = mail['account']
        body.password = mail['password']
        
        payload = Payload(header, body)
        
        self._handle_sync_request(payload)
        
        self.handle_logout_response()

    def handle_logout_response(self):
        self._handle_sync_response(dtp_struct.LogoutAccountResponse)


    def handle_send_order_request(self, mail):
        
        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.token = self._token
        body = dtp_struct.PlaceOrder()
        body.account_no = mail['account']
        body.order_original_id = generate_original_id()
        body.exchange = dtp_type.EXCHANGE_SH_A
        body.code = "601398"
        body.price = "5.5"
        body.quantity = 1000
        body.order_side = dtp_type.ORDER_SIDE_BUY
        body.order_type = dtp_type.ORDER_TYPE_LIMIT
        
        header.api_id = dtp_api_id.PLACE_ORDER
        
        order_payload = Payload(header, body)
    
        self._handle_async_request(order_payload)

    def handle_send_order_request_(self, mail):
        
        header = dtp_struct.RequestHeader()
        header.request_id = generate_request_id()
        header.token = self._token
        body = dtp_struct.PlaceOrder()
        body.account_no = mail['account']
        body.order_original_id = generate_original_id()
        body.exchange = mail['exchange']
        body.code = mail['code']
        body.price = mail['price']
        body.quantity = mail['quantity']
        body.order_side = mail['order_size']
        body.order_type = mail['order_type']
        
        header.api_id = dtp_api_id.PLACE_ORDER
        order_payload = Payload(header, body)
        
        self._handle_async_request(order_payload)
    
    def handle_counter_response(self):

        sock = self._async_resp_channel
        
        while self._running:

            topic = sock.recv()
            report_header = sock.recv()
            report_body = sock.recv()
            
            header = dtp_struct.ReportHeader()
            header.ParseFromString(report_header)
            
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
                print('unknown resp')
                continue
            
            handler_id = '{}_resp'.format(header.api_id)
            self.dispatcher.put({
                'handler_id': handler_id,
                'content': Payload(header, body)
            })

    

    def _handle_sync_request(self, payload):
        
        self._sync_req_resp_channel.send(
            payload.header.SerializeToString(), zmq.SNDMORE)
        
        self._sync_req_resp_channel.send(
            payload.body.SerializeToString())

        
    def _handle_sync_response(self, resp_type):
        _header = self._sync_req_resp_channel.recv()
        _body = self._sync_req_resp_channel.recv()

        response_header = dtp_struct.ResponseHeader()
        response_header.ParseFromString(_header)
        
        response_body = resp_type()
        response_body.ParseFromString(_body)
        payload = Payload(response_header, response_body)
        
        if resp_type is dtp_struct.LoginAccountResponse:
            self._token = payload.body.token
        
        self.dispatcher.put({
            'handler_id': '{}_resp'.format(response_header.api_id),
            'content': payload
        })

        
    def _handle_async_request(self, payload):
        
        self._async_req_channel.send(
            payload.header.SerializeToString(), zmq.SNDMORE)
        
        self._async_req_channel.send(
            payload.body.SerializeToString())


class Mail(object):
    
    def __init__(self, api_id, api_type, **kw):
        if 'handler_id' not in kw:
            kw['handler_id'] = '{}_{}'.format(api_id, api_type)
        self._kw = kw
        
    def __getitem__(self, key):
        return self._kw[key]

class Trader(object):
    
    def __init__(self):

        self._token = None

    def start(self):
        
        self.dispatcher = dispatcher = Dispatcher()
        self.broker = broker = DTP(dispatcher)
        
        dispatcher.bind(
            '{}_resp'.format(LOGIN_ACCOUNT_RESPONSE),
            self._on_login)
        
        dispatcher.bind(
            '{}_resp'.format(PLACE_REPORT), 
            self._on_order)
        
        dispatcher.bind(
            '{}_resp'.format(FILL_REPORT),
            self._on_trade)
        
        dispatcher.bind(
            '{}_resp'.format(QUERY_CAPITAL_RESPONSE),
            self._on_account)
        
        dispatcher.bind(
            '{}_req'.format(LOGIN_ACCOUNT_REQUEST),
            broker.handle_login_request)
        
        dispatcher.bind(
            '{}_req'.format(LOGOUT_ACCOUNT_REQUEST),
            broker.handle_logout_request)
        
        dispatcher.bind(
            '{}_req'.format(PLACE_ORDER),
            broker.handle_send_order_request)

    
    def _on_login(self, mail):
        response = mail['content']
        print(response.header)
        print(response.body)


        
    def _on_order(self, mail):
        response = mail['content']
        print(response.header)
        print(response.body)
    
    def _on_trade(self, mail):
        response = mail['content']
        print(response.header)
        print(response.body)
        
    def _on_account(self, mail):
        print('account:\n', mail)
        

    def login(self, account, password, **kw):
        mail = Mail(
            api_type='req',
            api_id=LOGIN_ACCOUNT_REQUEST,
            account=account,
            password=password
        )
        self.dispatcher.put(mail)
        
    def logout(self, account, password, **kw):
        mail = Mail(
            api_id=LOGOUT_ACCOUNT_REQUEST,
            account=account,
            password=password
        )
        self.dispatcher.put(mail)
    
    def send_order(self, order):
        """
        下单
        """
        mail = Mail(
            api_id=LOGIN_ACCOUNT_REQUEST,
            account=config['account']
        )
        self.dispatcher.put(mail)
    
    def send_order_batch(self, batch):
        """
        批量下单
        """
        pass
    
    def cancel_order(self, **kw):
        """
        撤单
        """
        pass
    
    def query_orders(self, **kw):
        """
        查询订单
        """
        pass
    
    def query_position(self, **kw):
        """
        查询持仓
        """
        pass
    
    def query_account(self, **kw):
        """
        查询账户
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


if __name__ == '__main__':

    login_mail = {
       'api_id':  LOGIN_ACCOUNT_REQUEST,
       'account': config['account'],
       'password': config['password']
    }
    
    trader = Trader()
    trader.start()
    trader.login(**login_mail)
