import zmq
import threading
import time

import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.dtp_api_id as dtp_api_id

import mock_data as my_data

class DtpServerSyncChannelMocker:
    def __init__(self):
        self.context = zmq.Context()
        self.socket_req = self.context.socket(zmq.REP)
        self.socket_req.bind("tcp://127.0.0.1:9003")

    def start(self):
        self._running = True
        threading.Thread(target=self._start_sync_channel).start()

    def terminate(self):
        self._running = False

    def _start_sync_channel(self):
        while self._running:
            try:
                # Wait for next request from client
                frame1 = self.socket_req.recv(flags=zmq.NOBLOCK)
                frame2 = self.socket_req.recv()
            except zmq.ZMQError as e:
                time.sleep(0.000001)
                continue

            request_header = dtp_struct.RequestHeader()
            request_header.ParseFromString(frame1)
            if(request_header.api_id == dtp_api_id.LOGIN_ACCOUNT_REQUEST):
                request_body = dtp_struct.LoginAccountRequest()
                request_body.ParseFromString(frame2)
                self._mock_login_process(request_header, request_body)
            elif(request_header.api_id == dtp_api_id.LOGOUT_ACCOUNT_REQUEST):
                request_body = dtp_struct.LogoutAccountRequest()
                request_body.ParseFromString(frame2)
                self._mock_logout_process(request_header, request_body)
            elif(request_header.api_id == dtp_api_id.QUERY_CAPITAL_REQUEST):
                request_body = dtp_struct.QueryCapitalRequest()
                request_body.ParseFromString(frame2)
                self._mock_query_capital_process(request_header, request_body)

    def _mock_login_process(self, request_header, request_body):
        response_header = dtp_struct.ResponseHeader()
        response_header.api_id = dtp_api_id.LOGIN_ACCOUNT_RESPONSE
        response_header.request_id = request_header.request_id
        if(request_body.account_no == my_data.account_no_empty):
            response_header.code = dtp_type.RESPONSE_CODE_BAD_REQUEST
            response_header.message = "account is empty"
            reponse_body = dtp_struct.LoginAccountResponse()
            payload = my_data.Payload(response_header, reponse_body)                
        elif(request_body.account_no not in my_data.account_no_pwd_dict):
            response_header.code = dtp_type.RESPONSE_CODE_UNAUTHORIZED
            response_header.message = "unknown account."
            reponse_body = dtp_struct.LoginAccountResponse()
            payload = my_data.Payload(response_header, reponse_body)
        elif(request_body.password == my_data.account_no_pwd_dict[request_body.account_no]):
            response_header.code = dtp_type.RESPONSE_CODE_OK
            response_header.message = "success."
            reponse_body = dtp_struct.LoginAccountResponse()
            reponse_body.token = my_data.account_no_token_dict[request_body.account_no]
            payload = my_data.Payload(response_header, reponse_body)
        else:
            response_header.code = dtp_type.RESPONSE_CODE_UNAUTHORIZED
            response_header.message = "password error."
            reponse_body = dtp_struct.LoginAccountResponse()
            payload = my_data.Payload(response_header, reponse_body)

        self.socket_req.send(payload.header.SerializeToString(), zmq.SNDMORE)
        self.socket_req.send(payload.body.SerializeToString())

    def _mock_logout_process(self, request_header, request_body):
        response_header = dtp_struct.ResponseHeader()
        response_header.api_id = dtp_api_id.LOGOUT_ACCOUNT_RESPONSE
        response_header.request_id = request_header.request_id
        if(request_body.account_no in my_data.account_no_pwd_dict):
            response_header.code = dtp_type.RESPONSE_CODE_OK
            response_header.message = "success"
            reponse_body = dtp_struct.LogoutAccountResponse()
            payload = my_data.Payload(response_header, reponse_body)
        else:
            response_header.code = dtp_type.RESPONSE_CODE_UNAUTHORIZED
            response_header.message = "unknow account"
            reponse_body = dtp_struct.LogoutAccountResponse()
            payload = my_data.Payload(response_header, reponse_body)

        self.socket_req.send(payload.header.SerializeToString(), zmq.SNDMORE)
        self.socket_req.send(payload.body.SerializeToString())

    def _mock_query_capital_process(self, request_header, request_body):
        response_header = dtp_struct.ResponseHeader()
        response_header.api_id = dtp_api_id.QUERY_CAPITAL_RESPONSE
        response_header.request_id = request_header.request_id
        if(request_body.account_no in my_data.account_no_pwd_dict):
            response_header.code = dtp_type.RESPONSE_CODE_OK
            response_header.message = "success"
            reponse_body = dtp_struct.QueryCapitalResponse()
            reponse_body.account_no = request_body.account_no
            payload = my_data.Payload(response_header, reponse_body)
        else:
            response_header.code = dtp_type.RESPONSE_CODE_UNAUTHORIZED
            response_header.message = "unknow account"
            reponse_body = dtp_struct.QueryCapitalResponse()
            reponse_body.account_no = request_body.account_no
            payload = my_data.Payload(response_header, reponse_body)

        self.socket_req.send(payload.header.SerializeToString(), zmq.SNDMORE)
        self.socket_req.send(payload.body.SerializeToString())

if __name__ == '__main__':
    mocker = DtpServerSyncChannelMocker()
    mocker.start()
    input("Enter any key to stop...")
    mocker.terminate()
