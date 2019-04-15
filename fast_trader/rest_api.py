# -*- coding: utf-8 -*-

import sys
import getpass
import json
import requests

import inflection

from fast_trader.settings import settings
from fast_trader.utils import get_local_ip



session = requests.Session()

user_meta = {}

if sys.platform == 'win32':
    
    def _read_kay(path):
        with open(path) as f:
            content = f.read()
        return dict(tuple(p.split('=')) for p in content.split('\n')[:-1])
    
    os_user = getpass.getuser()
    kay_file = 'c:/Users/{user}/AppData/Roaming/Kay/.kay'.format(
        user=os_user)
    
    user_meta.update(_read_kay(kay_file))
    
else:
    raise RuntimeError('Only Windows platform is currently supported')

default_headers = {
    'userId': 'undefined',  # FIXME
    'ip': user_meta.get('ip', get_local_ip()),
    'mac': user_meta['mac'],
    'harddisk': user_meta['harddisk'],
    'token': user_meta['token'],
}


def request(url, headers, body):
    headers_ = default_headers.copy()
    headers_.update(headers)
    data = json.dumps(body)
    r = session.post(url, headers=headers_, data=data)
    return json.loads(r.text)


def place_order(code, exchange, order_original_id,
                side, price, quantity, orderType):
    url = settings['rest_api']['order']
    headers = {}
    body = {
        "originalId": "string",
        "orderStatus": 0,
        "exchange": 0,
        "code": "string",
        "side": 0,
        "price": "string",
        "quantity": 0,
        "orderType": 0,
        "orderTime": 0
    }
    return request(url, headers=headers, body=body)


def cancel_order(order_original_id):
    # TODO: test non-optional params
    url = settings['rest_api']['cancel_order']
    headers = {}
    body = {
        "originalId": order_original_id,
        "orderTime": 0,
        "exchange": 0,
        "code": "",
        "cancelOrderExchangeId": "",
    }
    return request(url, headers=headers, body=body)


def place_batch_order(orders):
    url = settings['rest_api']['batch_order']
    headers = {}
    body = []

    for order in orders:
        order_ = {}
        body.append(order)
        for k, v in order.items():
            k_ = inflection.camelize(k, uppercase_first_letter=False)
            order_[k_] = v

    return request(url, headers=headers, body=body)


def cancel_batch_order():
    # TODO: 全撤？
    url = settings['rest_api']['cancel_batch']
    headers = {}
    body = {}
    return request(url, headers=headers, body=body)
