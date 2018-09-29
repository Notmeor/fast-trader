import random
import csv
import time

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
    dtp_subscribe_channel.register_counter_callback(
        support.place_report_callback, support.fill_report_callback, support.cancel_report_callback)

def login(account_no):
    header = dtp_struct.RequestHeader()
    header.request_id = random_request_id()
    body = dtp_struct.LoginAccountRequest()
    body.account_no = account_no
    body.password = support.get_pwd(account_no)
    payload = dtp.Payload(header, body)

    response_payload = dtp_sync_channel.login_account(payload)
    print("Login ResponseHeader:")
    print(response_payload.header)
    print("Login ResponseBody:")
    print(response_payload.body)
    support.set_token(body.account_no, response_payload.body.token)
    return response_payload.body.token

def place_order(order):
    header = dtp_struct.RequestHeader()
    header.request_id = random_request_id()
    account_no = order[5]
    header.token = support.get_token(account_no)
    body = dtp_struct.PlaceOrder()
    body.account_no = account_no
    body.order_original_id = random_original_id()
    body.exchange = dtp_type.EXCHANGE_SH_A if order[0] == 'sh' else dtp_type.EXCHANGE_SZ_A
    body.code = order[1]
    body.price = order[2]
    body.quantity = int(order[3])
    body.order_side = dtp_type.ORDER_SIDE_BUY if order[4] == 'buy' else dtp_type.ORDER_SIDE_SELL
    body.order_type = dtp_type.ORDER_TYPE_LIMIT
    order_payload = dtp.Payload(header, body)

    support.set_order_start_time(body.order_original_id)
    dtp_async_channel.place_order(order_payload)


def read_orders_from_csv(file):
    with open(support.ORDERS_FILE, "r", encoding = "utf-8") as f:
        reader = csv.reader(f)
        rows = [row for row in reader]
    return rows

def main():
    RECORD_TO_CVS = True
    connect_channels()
    register_callback()
    # dtp_subscribe_channel.start_subscribe_report(topic=support.ACCOUNT_NO)
    dtp_subscribe_channel.start_subscribe_report(topic="")

    # record report to csv
    support.enable_record_to_csv()

    print("################################# login account:")
    for account_no in support.account_token_dict:
        token = login(account_no)
        print("login response token:" + token)

    orders = read_orders_from_csv(support.ORDERS_FILE)

    for i in range(1, len(orders)):
        # print(orders[i])
        place_order(orders[i])
        time.sleep(0.05)

    print("################################# finished.")
    input("Enter any key to stop...")
    disconnect_channels()

    support.disable_record_to_csv()


if __name__ == '__main__':
    main()
