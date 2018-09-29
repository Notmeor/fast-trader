import time
import csv
import datetime

import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.skeleton as dtp

ACCOUNT_NO_1 = "021000062436"
ACCOUNT_NO_2 = "012000044335"

account_token_dict = {
    ACCOUNT_NO_1: {"pwd": "123456", "token" : ""},
    ACCOUNT_NO_2: {"pwd": "123456", "token" : ""},
}

ORDERS_FILE = "orders.csv"
REPORTS_COMPLIANCE_FILE = "report_compliance.csv"
REPORTS_PLACE_FILE = "report_place.csv"
REPORTS_FILL_FILE = "report_fill.csv"
REPORTS_CANCEL_FILE = "report_cancel.csv"

RECORD_TO_CSV = False
place_out=place_csv_writer=object()
fill_out=fill_csv_writer=object()
cancel_out=cancel_csv_writer=object()
compliance_out=compliance_csv_writer=object()

order_lagency_dict = {}

def set_token(account_no, token):
    global account_token_dict
    account_token_dict[account_no]["token"] = token

def get_token(account_no):
    return account_token_dict[account_no]["token"]

def get_pwd(account_no):
    return account_token_dict[account_no]["pwd"]

def enable_record_to_csv():
    global RECORD_TO_CSV
    global place_out, place_csv_writer, fill_out, fill_csv_writer
    global cancel_out, cancel_csv_writer, compliance_out, compliance_csv_writer
    RECORD_TO_CSV = True
    place_out = open(REPORTS_PLACE_FILE, "w", newline = "")
    place_csv_writer = csv.writer(place_out, dialect = "excel")
    fill_out = open(REPORTS_FILL_FILE, "w", newline = "")
    fill_csv_writer = csv.writer(fill_out, dialect = "excel")
    cancel_out = open(REPORTS_CANCEL_FILE, "w", newline = "")
    cancel_csv_writer = csv.writer(cancel_out, dialect = "excel")
    compliance_out = open(REPORTS_COMPLIANCE_FILE, "w", newline = "")
    compliance_csv_writer = csv.writer(compliance_out, dialect = "excel")

    place_csv_header = ["rec_time",
        "api_id", "response code", "response message",
        "order_exchange_id", "placed_time", "freeze_amount",
        "status", "order_original_id", "account_no",
        "exchange", "code", "quantity",
        "order_side"]
    compliance_csv_writer.writerow(place_csv_header)
    compliance_out.flush()
    place_csv_writer.writerow(place_csv_header)
    place_out.flush()

    fill_csv_header = ["rec_time",
        "api_id", "response code", "response message",
        "fill_exchange_id", "fill_time", "fill_status",
        "fill_price", "fill_quantity", "fill_amount",
        "clear_amount", "total_fill_quantity", "total_fill_amount",
        "total_cancelled_quantity", "order_exchange_id", "order_original_id",
        "account_no", "exchange", "code",
        "price", "quantity", "order_side"]
    fill_csv_writer.writerow(place_csv_header)
    fill_out.flush()

    cancel_csv_header = ["rec_time",
        "api_id", "response code", "response message",
        "order_exchange_id", "order_original_id", "account_no",
        "exchange", "code", "quantity",
        "order_side", "status", "total_fill_quantity",
        "cancelled_quantity", "freeze_amount"]
    cancel_csv_writer.writerow(cancel_csv_header)
    cancel_out.flush()

def disable_record_to_csv():
    global RECORD_TO_CSV
    RECORD_TO_CSV = False
    place_out.close()
    fill_out.close()
    cancel_out.close()
    compliance_out.close()

def set_order_start_time(order_original_id):
    global order_lagency_dict
    order_lagency_dict[order_original_id] = [datetime.datetime.now(), datetime.datetime.now()]

def compiance_failed_callback(response_payload):
    print(".............compiance_failed_callback")
    if(RECORD_TO_CSV):
        header = response_payload.header
        body = response_payload.body
        row = [time.strftime('%X'),
            header.api_id, header.code, header.message,
            body.order_exchange_id, body.placed_time, body.freeze_amount,
            body.status, body.order_original_id, body.account_no,
            body.exchange, body.code, body.quantity,
            body.order_side]
        print(row)
        compliance_csv_writer.writerow(row)
        compliance_out.flush()
    print(".............compliance header:")
    print(response_payload.header)
    print(".............compliance body:")
    print(response_payload.body)
    print(".............compiance_failed_callback finished!")

def place_report_callback(response_payload):
    print(".............place_report_callback")
    if(RECORD_TO_CSV):
        global order_lagency_dict
        header = response_payload.header
        body = response_payload.body
        order_lagency_dict[body.order_original_id][1] = datetime.datetime.now()
        lagency = (order_lagency_dict[body.order_original_id][1] - order_lagency_dict[body.order_original_id][0]).total_seconds()
        row = [lagency,
            header.api_id, header.code, header.message,
            body.order_exchange_id, body.placed_time, body.freeze_amount,
            body.status, body.order_original_id, body.account_no,
            body.exchange, body.code, body.quantity,
            body.order_side]
        print(row)
        place_csv_writer.writerow(row)
        place_out.flush()
    print(".............place header:")
    print(response_payload.header)
    print(".............place body:")
    print(response_payload.body)
    print(".............place_report_callback finished!")

def fill_report_callback(response_payload):
    print(".............fill_report_callback")
    if(RECORD_TO_CSV):
        global order_lagency_dict
        header = response_payload.header
        body = response_payload.body
        order_lagency_dict[body.order_original_id][1] = datetime.datetime.now()
        lagency = (order_lagency_dict[body.order_original_id][1] - order_lagency_dict[body.order_original_id][0]).total_seconds()
        row = [lagency,
            header.api_id, header.code, header.message,
            body.fill_exchange_id, body.fill_time, body.fill_status,
            body.fill_price, body.fill_quantity, body.fill_amount,
            body.clear_amount, body.total_fill_quantity, body.total_fill_amount,
            body.total_cancelled_quantity, body.order_exchange_id, body.order_original_id,
            body.account_no, body.exchange, body.code,
            body.price, body.quantity, body.order_side]
        print(row)
        fill_csv_writer.writerow(row)
        fill_out.flush()
    print(".............fill header:")
    print(response_payload.header)
    print(".............fill body:")
    print(response_payload.body)
    print(".............fill_report_callback finished!")

def cancel_report_callback(response_payload):
    print(".............cancel_report_callback")
    if(RECORD_TO_CSV):
        header = response_payload.header
        body = response_payload.body
        row = [time.strftime('%X'),
            header.api_id, header.code, header.message,
            body.order_exchange_id, body.order_original_id, body.account_no,
            body.exchange, body.code, body.quantity,
            body.order_side, body.status, body.total_fill_quantity,
            body.cancelled_quantity, body.freeze_amount]
        print(row)
        cancel_csv_writer.writerow(row)
        cancel_out.flush()
    print(".............cancel header:")
    print(response_payload.header)
    print(".............cancel body:")
    print(response_payload.body)
    print(".............cancel_report_callback finished!")
