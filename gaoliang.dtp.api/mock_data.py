import dtp.type_pb2 as dtp_type
import random


class Order():
    def __init__(self, account_no, original_id, exchange, code, price, quantity, order_side, order_type):
        self.account_no = account_no
        self.original_id = original_id
        self.exchange = exchange
        self.code = code
        self.price = price
        self.quantity = quantity
        self.order_side = order_side
        self.order_type = order_type

class PlacedReport():
    def __init__(self, exchange_id, placed_time, freeze_amount, status, original_id, account_no, exchange, code, quantity, order_side):
        self.exchange_id = exchange_id
        self.placed_time = placed_time
        self.freeze_amount = freeze_amount
        self.status = status
        self.original_id = original_id
        self.account_no = account_no
        self.exchange = exchange
        self.code = code
        self.quantity = quantity
        self.order_side = order_side

class FillReport():
    def __init__(self, fill_exchange_id, fill_time, fill_status, fill_price, fill_quantity,
        fill_amount, clear_amount, total_fill_quantity, total_fill_amount, total_cancelled_quantity,
        order_exchange_id, order_original_id, account_no, exchange, code, price, quantity, order_side):
        self.fill_exchange_id = fill_exchange_id
        self.fill_time = fill_time
        self.fill_status = fill_status
        self.fill_price = fill_price
        self.fill_quantity = fill_quantity
        self.fill_amount = fill_amount
        self.clear_amount = clear_amount
        self.total_fill_quantity = total_fill_quantity
        self.total_fill_amount = total_fill_amount
        self.total_cancelled_quantity = total_cancelled_quantity
        self.order_exchange_id = order_exchange_id
        self.order_original_id = order_original_id
        self.account_no = account_no
        self.exchange = exchange
        self.code = code
        self.price = price
        self.quantity = quantity
        self.order_side = order_side


account_no_a = "100900501"
account_no_b = "100900502"
account_no_c = "100900503"

account_no_pwd_dict = {
    account_no_a: "xxxx&xxxx",
    account_no_b: "yyyy&yyyy",
    account_no_c: "zzzz&zzzz",
}

account_no_token_dict = {
    account_no_a: "xxxx-xxxx-xxxx-xxxx",
    account_no_b: "yyyy-yyyy-yyyy-yyyy",
    account_no_c: "zzzz-zzzz-zzzz-zzzz",
}

account_no_empty = ""
account_no_unknown = "unknown_account"
pwd_unkown = "unkown_pwd"
token_unknown = "vvvv-vvvv-vvvv-vvvv"


def generate_request_id():
    return str(random.randrange(11000000,11900000))

def generate_original_id():
    return str(random.randrange(61000000,61900000))

def get_account_pwd(account_no):
    return account_no_pwd_dict[account_no]

normal_order_01 = Order(account_no_a, "61001091", dtp_type.EXCHANGE_SH_A, "600887", "28.91", 5000, 
    dtp_type.ORDER_SIDE_BUY, dtp_type.ORDER_TYPE_LIMIT)

normal_order_01_placing = PlacedReport("71001091", "13:55:20", normal_order_01.quantity,
    dtp_type.ORDER_STATUS_PLACING, normal_order_01.original_id, normal_order_01.account_no,
    normal_order_01.exchange, normal_order_01.code, normal_order_01.quantity, normal_order_01.order_side)

normal_order_01_fill = FillReport("81001091", "13:55:30", dtp_type.FILL_STATUS_FILLED, "28.90", 5000,
    "144500.00", "144505.00", 5000, "144500.00", 0, normal_order_01_placing.exchange_id, normal_order_01_placing.original_id,
    normal_order_01.account_no, normal_order_01.exchange, normal_order_01.code, normal_order_01.price,
    normal_order_01.quantity, normal_order_01.order_side)

# for compliance fail
unnormal_order_01 = Order(account_no_a, "61001092", dtp_type.EXCHANGE_SH_A, "600887", "28.91", 50000000, 
    dtp_type.ORDER_SIDE_BUY, dtp_type.ORDER_TYPE_LIMIT)

class Payload(object):
    def __init__(self, header, body):
        self.header = header
        self.body = body

