import random

import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.skeleton as dtp

import sample_support as support


dtp_sync_channel = dtp.DtpSyncChannel()

def random_request_id():
    return str(random.randrange(11000000,11900000))

def connect_channels():
    dtp_sync_channel.connect()

def disconnect_channels():
    dtp_sync_channel.disconnect()

def login():
    header = dtp_struct.RequestHeader()
    header.request_id = random_request_id()
    body = dtp_struct.LoginAccountRequest()
    body.account_no = support.ACCOUNT_NO_1
    body.password = support.get_pwd(body.account_no)
    payload = dtp.Payload(header, body)

    response_payload = dtp_sync_channel.login_account(payload)
    print("Login ResponseHeader:")
    print(response_payload.header)
    print("Login ResponseBody:")
    print(response_payload.body)
    support.set_token(body.account_no, response_payload.body.token)
    return response_payload.body.token

def query_capital(token):
    header = dtp_struct.RequestHeader()
    header.request_id = random_request_id()
    header.token = token
    body = dtp_struct.QueryCapitalRequest()
    body.account_no = support.ACCOUNT_NO_1
    payload = dtp.Payload(header, body)

    response_payload = dtp_sync_channel.query_capital(payload)
    print("Query Capital ResponseHeader:")
    print(response_payload.header)
    print("Query Capital ResponseBody:")
    print(response_payload.body)

def query_position(token):
    header = dtp_struct.RequestHeader()
    header.request_id = random_request_id()
    header.token = token
    body = dtp_struct.QueryPositionRequest()
    body.account_no = support.ACCOUNT_NO_1
    payload = dtp.Payload(header, body)

    response_payload = dtp_sync_channel.query_position(payload)
    print("Query Position ResponseHeader:")
    print(response_payload.header)
    print("Query Posiiton ResponseBody:")
    print(response_payload.body)

def place_order():
    pass

def main():
    connect_channels()

    token = login()
    print("login response token:" + token)
    token = support.get_token(support.ACCOUNT_NO_1)
    print("login response token:" + token)
    query_capital(token)
    query_position(token)

    disconnect_channels()

if __name__ == '__main__':
    main()
