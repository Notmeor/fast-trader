import zmq
import time
import threading
from queue import Queue

import dtp.dtp_api_id as dtp_api_id
import dtp.api_pb2 as dtp_struct

# config:
# python -m unittest
SYNC_CHANNEL_PORT = "tcp://localhost:9003"
# ASYNC_CHANNEL_PORT = "tcp://localhost:9001"     # connecct adapter
ASYNC_CHANNEL_PORT = "tcp://localhost:9101"     # connect compliance
SUB_COUNTER_PORT = "tcp://localhost:9002"
SUB_COMPLIANCE_PORT = "tcp://localhost:9102"

# # config:
# # gaoliang.wenbo
# SYNC_CHANNEL_PORT = "tcp://localhost:9003"
# ASYNC_CHANNEL_PORT = "tcp://localhost:9001"     # connecct adapter
# # ASYNC_CHANNEL_PORT = "tcp://localhost:9101"     # connect compliance
# SUB_COUNTER_PORT = "tcp://localhost:9002"
# SUB_COMPLIANCE_PORT = "tcp://localhost:9102"

# # config:
# #gaoliang.matching
# SYNC_CHANNEL_PORT = "tcp://192.168.221.52:9203"
# #ASYNC_CHANNEL_PORT = "tcp://192.168.221.52:9201"     # connect adapter
# ASYNC_CHANNEL_PORT = "tcp://192.168.221.52:9101"     # connect compliance
# SUB_COUNTER_PORT = "tcp://192.168.221.52:9202"
# SUB_COMPLIANCE_PORT = "tcp://192.168.221.52:9102"


class DtpSyncChannel(object):
    def __init__(self):
        self.context = zmq.Context()
        self.socket_req = self.context.socket(zmq.REQ)

    def connect(self):
        print("connecting dtp sync channel...")
        self.socket_req.connect(SYNC_CHANNEL_PORT)

    def disconnect(self):
        print("disconnecting dtp sync channel...")
        self.socket_req.disconnect(SYNC_CHANNEL_PORT)

    def login_account(self, payload):
        return self._process_invoke(payload, dtp_api_id.LOGIN_ACCOUNT_REQUEST, dtp_api_id.LOGIN_ACCOUNT_RESPONSE)

    def logout_account(self, payload):
        return self._process_invoke(payload, dtp_api_id.LOGOUT_ACCOUNT_REQUEST, dtp_api_id.LOGOUT_ACCOUNT_RESPONSE)

    def query_orders(self, payload):
        return self._process_invoke(payload, dtp_api_id.QUERY_ORDERS_REQUEST, dtp_api_id.QUERY_ORDERS_RESPONSE)

    def query_fills(self, payload):
        return self._process_invoke(payload, dtp_api_id.QUERY_FILLS_REQUEST, dtp_api_id.QUERY_FILLS_RESPONSE)

    def query_capital(self, payload):
        return self._process_invoke(payload, dtp_api_id.QUERY_CAPITAL_REQUEST, dtp_api_id.QUERY_CAPITAL_RESPONSE)

    def query_position(self, payload):
        return self._process_invoke(payload, dtp_api_id.QUERY_POSITION_REQUEST, dtp_api_id.QUERY_POSITION_RESPONSE)

    def query_ration(self, payload):
        return self._process_invoke(payload, dtp_api_id.QUERY_RATION_REQUEST, dtp_api_id.QUERY_RATION_RESPONSE)

    def _process_invoke(self, payload, request_api_id, response_api_id):
        payload.header.api_id = request_api_id
        self.socket_req.send(payload.header.SerializeToString(), zmq.SNDMORE)
        self.socket_req.send(payload.body.SerializeToString())

        header = self.socket_req.recv()
        body = self.socket_req.recv()

        response_header = dtp_struct.ResponseHeader()
        response_header.ParseFromString(header)
        assert(response_header.api_id == response_api_id)
        if(response_header.api_id == dtp_api_id.LOGIN_ACCOUNT_RESPONSE):
            response_body = dtp_struct.LoginAccountResponse()
        elif(response_header.api_id == dtp_api_id.LOGOUT_ACCOUNT_RESPONSE):
            response_body = dtp_struct.LogoutAccountResponse()
        elif(response_header.api_id == dtp_api_id.QUERY_ORDERS_RESPONSE):
            response_body = dtp_struct.QueryOrdersResponse()
        elif(response_header.api_id == dtp_api_id.QUERY_FILLS_RESPONSE):
            response_body = dtp_struct.QueryFillsResponse()
        elif(response_header.api_id == dtp_api_id.QUERY_CAPITAL_RESPONSE):
            response_body = dtp_struct.QueryCapitalResponse()
        elif(response_header.api_id == dtp_api_id.QUERY_POSITION_RESPONSE):
            response_body = dtp_struct.QueryPositionResponse()
        elif(response_header.api_id == dtp_api_id.QUERY_RATION_RESPONSE):
            response_body = dtp_struct.QueryRationResponse()
        else:
            assert(False)
        response_body.ParseFromString(body)
        response_payload = Payload(response_header, response_body)
        return response_payload



class DtpAsyncChannel(object):
    def __init__(self):
        self.context = zmq.Context()
        self.socket_dealer = self.context.socket(zmq.DEALER)

    def connect(self):
        print("connecting dtp async channel...start")
        self.socket_dealer.connect(ASYNC_CHANNEL_PORT)
        print("connecting dtp async channel...finished")

    def disconnect(self):
        print("disconnecting dtp async channel...")
        self.socket_dealer.disconnect(ASYNC_CHANNEL_PORT)

    def place_order(self, payload):
        self._process_invoke(payload, dtp_api_id.PLACE_ORDER)

    def cancel_order(self, payload):
        self._process_invoke(payload, dtp_api_id.CANCEL_ORDER)

    def place_batch_order(self, payload):
        self._process_invoke(payload, dtp_api_id.PLACE_BATCH_ORDER)

    def _process_invoke(self, payload, async_request_api_id):
        payload.header.api_id = async_request_api_id
        self.socket_dealer.send(payload.header.SerializeToString(), zmq.SNDMORE)
        self.socket_dealer.send(payload.body.SerializeToString())



class DtpSubscribeChannel(object):
    def __init__(self, subcribe_interval=0.00001):
        # default subcribe_interval is 10um
        self.subcribe_interval = subcribe_interval
        self.context = zmq.Context()
        self.socket_sub_counter_report = self.context.socket(zmq.SUB)
        self.socket_sub_compliance_report = self.context.socket(zmq.SUB)

    def connect(self):
        print("connecting dtp subcribe channel...")
        self.socket_sub_counter_report.connect(SUB_COUNTER_PORT)
        self.socket_sub_compliance_report.connect(SUB_COMPLIANCE_PORT)

    def disconnect(self):
        print("disconnecting dtp subcribe channel...")
        self.stop_subscribe_report()
        self.socket_sub_counter_report.disconnect(SUB_COUNTER_PORT)
        self.socket_sub_compliance_report.disconnect(SUB_COMPLIANCE_PORT)

    def register_compliance_callback(self, compiance_failed_callback):
        self._compiance_failed_callback = compiance_failed_callback

    def register_counter_callback(self, place_report_callback, fill_report_callback, cancel_report_callback):
        self._place_report_callback = place_report_callback
        self._fill_report_callback = fill_report_callback
        self._cancel_report_callback = cancel_report_callback

    def start_subscribe_report(self, topic):
        self._running = True
        self.socket_sub_counter_report.setsockopt_string(zmq.SUBSCRIBE, topic)
        threading.Thread(target=self._subscribe_counter_report).start()
        self.socket_sub_compliance_report.setsockopt_string(zmq.SUBSCRIBE, topic)
        threading.Thread(target=self._subscribe_compliance_report).start()

    def stop_subscribe_report(self):
        self._running = False

    def _subscribe_compliance_report(self):
        print("subcribing compliance report...")
        while self._running:
            try:
                topic = self.socket_sub_compliance_report.recv(flags=zmq.NOBLOCK)
                report_header = self.socket_sub_compliance_report.recv()
                report_body = self.socket_sub_compliance_report.recv()
                print("subcribed compliance report...")
            except zmq.ZMQError as e:
                time.sleep(self.subcribe_interval)
                continue
            header = dtp_struct.ReportHeader()
            header.ParseFromString(report_header)
            body = dtp_struct.PlacedReport()
            body.ParseFromString(report_body)
            self._compiance_failed_callback(Payload(header, body))

    def _subscribe_counter_report(self):
        print("subcribing counter report...")
        while self._running:
            try:
                topic = self.socket_sub_counter_report.recv(flags=zmq.NOBLOCK)
                report_header = self.socket_sub_counter_report.recv()
                report_body = self.socket_sub_counter_report.recv()
                print("subcribed counter report...")
            except zmq.ZMQError as e:
                time.sleep(self.subcribe_interval)
                continue
            self._distribute_counter_report_by_header(report_header, report_body)

    def _distribute_counter_report_by_header(self, report_header, report_body):
        header = dtp_struct.ReportHeader()
        header.ParseFromString(report_header)
        if(header.api_id == dtp_api_id.PLACE_REPORT):
            body = dtp_struct.PlacedReport()
            body.ParseFromString(report_body)
            self._place_report_callback(Payload(header, body))
        elif(header.api_id == dtp_api_id.FILL_REPORT):
            body = dtp_struct.FillReport()
            body.ParseFromString(report_body)
            self._fill_report_callback(Payload(header, body))
        elif(header.api_id == dtp_api_id.CANCEL_REPORT):
            body = dtp_struct.CancellationReport()
            body.ParseFromString(report_body)
            self._cancel_report_callback(Payload(header, body))



class Payload(object):
    def __init__(self, header, body):
        self.header = header
        self.body = body
