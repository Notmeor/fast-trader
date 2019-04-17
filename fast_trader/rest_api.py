# -*- coding: utf-8 -*-

import sys
import functools
import getpass
import json
import requests

import inflection

from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.settings import settings
from fast_trader.utils import get_local_ip, AnnotationCheckMixin, attrdict


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
    'Content-Type': 'application/json; charset=utf8',
    'ip': user_meta.get('ip', get_local_ip()),
    'mac': user_meta['mac'],
    'harddisk': user_meta['harddisk'].strip(),
    'token': user_meta['token'],
}

default_query_headers = {
    'Content-Type': default_headers['Content-Type'],
    'token': default_headers['token'],
}


def request(url, headers, body, method='post'):
    data = json.dumps(body)
    meth = getattr(session, method)
    r = meth(url, headers=headers, data=data, verify=False)
    if r.status_code not in [200, 201]:
        raise requests.HTTPError(f'{r.status_code}')
    if r.text:
        return json.loads(r.text)
    return r.text


def get_account():
    url = settings['rest_api']['get_account'].format(
        account_no=settings['account'])
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')
                 
                 
class Order(AnnotationCheckMixin):
    code: str
    exchange: int
    original_id: str
    order_type: int
    price: str
    quantity: int
    side: int
    
    def to_dict(self):
        self._check_fields
        return self.__dict__
       

def place_order(order):
    url = settings['rest_api']['order'].format(
        account_no=settings['account'])
    headers = default_headers
    body = {}
    for k, v in order.to_dict().items():
        k_ = inflection.camelize(k, uppercase_first_letter=False)
        body[k_] = v
    return request(url, headers=headers, body=body)


def cancel_order(order_exchange_id, order_original_id=''):
    # TODO: test non-optional params
    url = settings['rest_api']['cancel_order'].format(
        account_no=settings['account'])
    headers = default_headers
    body = {
        "originalId": order_original_id,
        "orderTime": 0,
        "exchange": 0,
        "code": "",
        "cancelOrderExchangeId": order_exchange_id,
    }
    return request(url, headers=headers, body=body)


def place_batch_order(orders):
    url = settings['rest_api']['batch_order'].format(
        account_no=settings['account'])
    headers = default_headers
    body = []

    for order in orders:
        order_ = {}
        body.append(order_)
        for k, v in order.to_dict().items():
            k_ = inflection.camelize(k, uppercase_first_letter=False)
            order_[k_] = v
    print(body)
    return request(url, headers=headers, body=body)


def cancel_batch_order(p):
    """
    批量撤单
    
    p: dict
        {
            '0': [],
            '1': []
        }
    """
    url = settings['rest_api']['cancel_batch'].format(
        account_no=settings['account'])
    headers = default_headers
    body = p
    return request(url, headers=headers, body=body)


def cancel_all():
    p = query_open_orders()
    cancel_batch_order(p)


def query_capital():
    url = settings['rest_api']['query_capital'].format(
        account_no=settings['account'])
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def query_positions():
    url = settings['rest_api']['query_positions'].format(
        account_no=settings['account'])
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def query_fills():
    url = settings['rest_api']['query_fills'].format(
        account_no = settings['account'])
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def query_orders():
    url = settings['rest_api']['query_orders'].format(
        account_no = settings['account'])
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def query_open_orders():
    url = settings['rest_api']['query_open_orders'].format(
        account_no = settings['account'])
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def restapi_login(trader, account, password, *args, **kw):
    stats = get_account()[0]
    print(account == stats['cashAccountNo'], account, stats['cashAccountNo'])
    if stats['loginStatus'] == 1:
        trader._logined = True
        trader._token = user_meta['token']
        print('Login success')
    else:
        print('Login failed')


def _get_order_obj(kw):
    order = Order()
    order.code = kw['code']
    order.exchange = kw['exchange']
    order.order_type = kw['order_type']
    order.original_id = kw['order_original_id']
    order.price = kw['price']
    print(order.price, type(order.price))
    order.quantity = kw['quantity']
    order.side = kw['order_side']
    return order


#def restapi_place_order(trader, request_id, order_original_id, exchange,
#                        code, price, quantity, order_side,
#                        order_type=dtp_type.ORDER_TYPE_LIMIT):
#    order = Order()
#    order.code = code
#    order.exchange = exchange
#    order.order_type = order_type
#    order.original_id = order_original_id
#    order.price = price
#    order.quantity = quantity
#    order.side = order_side
#    
#    place_order(order)


def restapi_place_order(trader, order_type=dtp_type.ORDER_TYPE_LIMIT, **kw):
    kw['order_type'] = order_type
    order = _get_order_obj(kw)
    place_order(order)


def restapi_cancel_order(trader, **kw):
    order_exchange_id = kw['order_exchange_id']
    cancel_order(order_exchange_id=order_exchange_id)
    


def restapi_place_batch_order(trader, request_id, orders):
    order_objs = []
    for kw in orders:
        order = _get_order_obj(kw)
        order_objs.append(order)
    place_batch_order(order_objs)


def restapi_query_capital(trader, **kw):
    stats = query_capital()[0]
    capital = {
        'account_no': stats['accountId'],
        'available': stats['available'],
        'balance': stats['balance'],
        'freeze': stats['freeze'],
        'securities': stats['marketValue'],
        'total': stats['total']
    }
    mail = {'body': capital}
    return mail


def restapi_query_positions(trader, **kw):
    stats = query_positions()
    positions = []
    for item in stats:
        pos = {
            'available_quantity': item['availableQuantity'],
            'balance': item['balance'],
            'buy_quantity': item['buyQuantity'],
            'code': item['code'],
            'cost': item['cost'],
            'exchange': item['exchange'],
            'freeze_quantity': item['freezeQuantity'],
            'market_value': item['marketValue'],
            'name': item['name'],
            'sell_quantity': item['sellQuantity']
        }
        positions.append(pos)
    mail = {'body': positions}
    return mail


def restapi_query_fills(trader, **kw):
    # TODO: pagination
    stats = query_fills()
    fills = []
    for item in stats:
        fill = {
            'clear_amount': item['clearAmount'],
            'code': item['code'],
            'exchange': item['exchange'],
            'fill_amount': item['fillAmount'],
            'fill_exchange_id': item['fillId'],
            'fill_price': item['fillPrice'],
            'fill_quantity': item['fillQuantity'],
            'fill_status': item['fillType'],
            'fill_time': item['fillTime'],
            'name': item['name'],
            'order_exchange_id': item['orderExchangeId'],
            'order_original_id': item['orderOriginalId'],
            'order_side': item['side']
        }
        fill = attrdict(fill)
        fills.append(fill)
    mail = attrdict()

    mail['body'] = attrdict()
    mail['body']['fill_list'] = fills
    
    pag = attrdict()
    pag['offset'] = len(fills)
    pag['size'] = 100
    if pag['offset'] > pag['size']:
        raise RuntimeError('Pagination Error')
    mail['body']['pagination'] = pag

    return mail


def _convert_order(kw):
    order = {
        'account_no': kw['accountNo'],
        'average_fill_price': kw['averageFillPrice'],
        'clear_amount': kw['clearAmount'],
        'code': kw['code'],
        'exchange': kw['exchange'],
        'freeze_amount': kw['freezeAmount'],
        'name': kw['name'],
        'order_exchange_id': kw['exchangeId'],
        'order_original_id': kw['originalId'],
        'order_side': kw['side'],
        'order_time': kw['orderTime'],
        'order_type': kw['orderType'],
        'price': kw['price'],
        'quantity': kw['quantity'],
        'status': kw['orderStatus'],
        'status_message': '',
        'total_cancelled_quantity': kw['cancelQuantity'],
        'total_fill_amount': kw['fillAmount'],
        'total_fill_quantity': kw['fillQuantity']
    }
    return order


def restapi_query_orders(trader, **kw):
    # TODO: pagination
    stats = query_orders()
    orders = []
    for item in stats:
        order = _convert_order(item)
        orders.append(order)
    mail = {'body': orders}
    return mail


def restapi_query_open_orders(trader, **kw):
    # TODO: pagination
    stats = query_open_orders()
    open_orders = []
    for item in stats:
        order = _convert_order(item)
        open_orders.append(order)
    mail  = {'body': open_orders}
    return mail


def might_use_rest_api(might, api_name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            if not might:
                ret = func(*args, **kw)
            else:
                api = globals()[api_name]
                ret = api(*args, **kw)
            return ret
        return wrapper
    return decorator
      