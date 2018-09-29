import sys
import random

import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.skeleton as dtp

import sample_support as support

dtp_sync_channel = dtp.DtpSyncChannel()
dtp_async_channel = dtp.DtpAsyncChannel()
dtp_subscribe_channel = dtp.DtpSubscribeChannel()

def random_request_id():
    return str(random.randrange(11000000,11900000))

def random_original_id():
    return str(random.randrange(61000000,61900000))

def connect_channels():
    dtp_sync_channel.connect()
    dtp_async_channel.connect()
    dtp_subscribe_channel.connect()

def disconnect_channels():
    dtp_sync_channel.disconnect()
    dtp_async_channel.disconnect()
    dtp_subscribe_channel.disconnect()

def register_callback():
    dtp_subscribe_channel.register_compliance_callback(support.compiance_failed_callback)
    dtp_subscribe_channel.register_counter_callback(support.place_report_callback,
        support.fill_report_callback, support.cancel_report_callback)

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
    return response_payload.body.token

def cancel_order(token, order_exchange_id):
    header = dtp_struct.RequestHeader()
    header.request_id = random_request_id()
    header.token = token
    body = dtp_struct.CancelOrder()
    body.account_no = support.ACCOUNT_NO_1
    body.exchange = dtp_type.EXCHANGE_SH_A
    body.order_exchange_id = order_exchange_id
    order_payload = dtp.Payload(header, body)

    dtp_async_channel.cancel_order(order_payload)

def main():
    order_exchange_id_to_cancel = sys.argv[1]

    connect_channels()
    register_callback()
    dtp_subscribe_channel.start_subscribe_report(topic=support.ACCOUNT_NO_1)

    print("################################# login account:")
    token = login()
    print("login response token:" + token)

    token = support.get_token(support.ACCOUNT_NO_1)
    print("login response token:" + token)
    print("############################################################")
    cancel_order(token, order_exchange_id_to_cancel)

    input("Enter any key to stop...")
    print("################################# finished.")
    disconnect_channels()

if __name__ == '__main__':
    main()
