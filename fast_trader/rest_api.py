# -*- coding: utf-8 -*-

import sys
import time
import functools
import getpass
import json
import requests
import logging
from urllib.parse import urlsplit
import inflection

from fast_trader.dtp import type_pb2 as dtp_type
from fast_trader.settings import settings
from fast_trader.utils import get_local_ip, AnnotationCheckMixin, attrdict

os_user = getpass.getuser()

session = requests.Session()

user_meta = {}
default_headers = {}
default_query_headers = default_headers

logger = logging.getLogger('rest_api')

REQUEST_TIMEOUT = 3


class RestSettings:

    def __init__(self):

        self._user_meta = user_meta
        self._default_headers = default_headers
        self._default_query_headers = default_query_headers

        self.settings_loaded = False
        self.logger = logging.getLogger('settings')

    def reload(self):
        # load kay file
        kay_file = settings['rest_api']['kay_file'].format(user=os_user)
        self._user_meta.update(self._read_kay(kay_file))
        self._user_meta['ip'] = get_local_ip()

        # construct default headers
        self._default_headers.update({
            'Content-Type': 'application/json; charset=utf8',
            'ip': user_meta['ip'],
            'mac': user_meta['mac'],
            'harddisk': user_meta['harddisk'],
            'token': user_meta['token'],
        })

        # 设定账户
        try:
            accounts = get_accounts()
#            settings.set({'account': accounts[0]['cashAccountNo']})
#            if len(accounts) > 1:
#                self.logger.warning(
#                    f'存在多个账号，默认使用{settings["account"]}')
        except:
            self.logger.error('读取账号失败', exc_info=True)
        else:
            self.settings_loaded = True

    @property
    def user_meta(self):
        return self._user_meta

    @property
    def default_headers(self):
        return self._default_headers

    @property
    def default_query_headers(self):
        return self._default_query_headers

    def _read_kay(self, path):
        with open(path) as f:
            content = f.read()
        content = dict(tuple(p.split('=')) for p in content.split('\n')[:-1])
        content['harddisk'] = content['harddisk'].strip()
        return content


class Order(AnnotationCheckMixin):
    code: str
    exchange: int
    original_id: str
    order_type: int
    price: str
    quantity: int
    side: int

    def to_dict(self):
        self._check_fields()
        return self.__dict__


def retry_on_proxy_error(func):
    @functools.wraps(func)
    def decorator(*args, **kw):
        try:
            return func(*args, **kw)
        except requests.exceptions.ProxyError:
            logger.error(f'Failed: {[args, kw]}', exc_info=True)
            time.sleep(1)
            return func(*args, **kw)
    return decorator


@retry_on_proxy_error
def request(url, headers, body, method='post'):
    logger.info(f'{method.upper()} {urlsplit(url).path}: {body}')
    data = json.dumps(body)
    meth = getattr(session, method)
    r = meth(url, headers=headers, data=data,
             verify=False, timeout=REQUEST_TIMEOUT)

    if r.status_code not in [200, 201]:
        raise requests.HTTPError(f'{r.status_code}, {r.text}')
    if r.text:
        return json.loads(r.text)
    # TODO: raise if `[null]` is returned?
    return r.text


def get_accounts():
    url = settings['rest_api']['get_account']
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def place_order(order, account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['order'].format(
        account_no=account_no)
    headers = default_headers
    body = {}
    for k, v in order.to_dict().items():
        k_ = inflection.camelize(k, uppercase_first_letter=False)
        body[k_] = v
    return request(url, headers=headers, body=body)


def cancel_order(exchange, order_exchange_id,
                 order_original_id='', account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['cancel_order'].format(
        account_no=account_no)
    headers = default_headers
    body = {
        "originalId": order_original_id,
        "orderTime": 0,
        "exchange": exchange,
        "code": "",
        "cancelOrderExchangeId": order_exchange_id,
    }
    return request(url, headers=headers, body=body)


def place_batch_order(orders, account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['batch_order'].format(
        account_no=account_no)
    headers = default_headers
    body = []

    for order in orders:
        order_ = {}
        body.append(order_)
        for k, v in order.to_dict().items():
            k_ = inflection.camelize(k, uppercase_first_letter=False)
            order_[k_] = v

    return request(url, headers=headers, body=body)


def cancel_batch_order(p, account_no=None):
    """
    批量撤单

    p: dict
        {
            '0': [],
            '1': []
        }
    """
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['cancel_batch'].format(
        account_no=account_no)
    headers = default_headers
    body = p
    return request(url, headers=headers, body=body)


def cancel_all(account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    p = query_open_orders(account_no=account_no)
    cancel_batch_order(p, account_no=account_no)


def query_capital(account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['query_capital'].format(
        account_no=account_no)
    headers = default_query_headers
    body = {}
    ret = request(url, headers=headers, body=body, method='get')
    # 查询特定账号的资金时，返回的依然是[]
    if isinstance(ret, list):
        return ret[0]
    return ret


def query_capitals():
    """
    查询所有账户资金
    """
    url = settings['rest_api']['query_capital'].format(
        account_no='')
    headers = default_query_headers
    body = {}
    ret = request(url, headers=headers, body=body, method='get')
    return ret


def query_positions(account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['query_positions'].format(
        account_no=account_no)
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def query_fills(account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['query_fills'].format(
        account_no=account_no)
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def query_orders(account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['query_orders'].format(
        account_no=account_no)
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def query_open_orders(account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api']['query_open_orders'].format(
        account_no=account_no)
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def _get_order_obj(kw):
    order = Order()
    order.code = kw['code']
    order.exchange = kw['exchange']
    order.order_type = kw['order_type']
    order.original_id = kw['order_original_id']
    order.price = kw['price']
    order.quantity = kw['quantity']
    order.side = kw['order_side']
    return order


def _get_by_account(result, account_no, name):
    return next(filter(
        lambda x: x[name] == account_no,
        result))


def make_pageable_query(method_name, page, size, account_no=None):
    if account_no is None:
        account_no = settings['acocunt_no']

    url = settings['rest_api'][method_name].format(
        account_no=account_no)
    url = f'{url}?page={page}&size={size}'
    headers = default_query_headers
    body = {}
    return request(url, headers=headers, body=body, method='get')


def handle_pagination(method_name, content_name,
                      pagination, format_fn, account_no):
    """
    FIXME: by account_no
    """
    size = pagination['size']
    offset = pagination['offset']
    page = int((offset + 1) / size)

    result = make_pageable_query(method_name, page,
                                 size, account_no=account_no)

    content = format_fn(result)

    mail = attrdict()
    mail['body'] = attrdict()
    mail['body'][content_name] = content

    pag = attrdict()
    pag['offset'] = offset + len(content)
    pag['size'] = size

    mail['body']['pagination'] = pag

    return mail


def format_positions(stats):
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
        positions.append(attrdict(pos))
    return positions


def format_fills(stats):
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
    return fills


def format_orders(stats):
    orders = []
    for kw in stats:
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
        orders.append(attrdict(order))
    return orders


def restapi_login(trader, account_no, password, *args, **kw):
    stats = _get_by_account(
            get_accounts(), account_no, 'cashAccountNo')
    if stats['loginStatus'] == 1:
        trader._account_no = account_no
        trader._logined = True
        trader._token = user_meta['token']
        print('连接账户成功')
    else:
        print('连接账户失败')

    trader.start()


def restapi_place_order(trader, order_type=dtp_type.ORDER_TYPE_LIMIT, **kw):
    kw['order_type'] = order_type
    order = _get_order_obj(kw)
    place_order(order, account_no=trader.account_no)


def restapi_cancel_order(trader, **kw):
    exchange = kw['exchange']
    order_exchange_id = kw['order_exchange_id']
    cancel_order(exchange=exchange,
                 order_exchange_id=order_exchange_id,
                 account_no=trader.account_no)


def restapi_place_batch_order(trader, request_id, orders):
    order_objs = []
    for kw in orders:
        order = _get_order_obj(kw)
        order_objs.append(order)
    place_batch_order(order_objs, account_no=trader.account_no)


def restapi_query_capital(trader, **kw):
    stats = query_capital(trader.account_no)
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
    return handle_pagination(
        method_name='query_positions',
        content_name='position_list',
        pagination=kw['pagination'],
        format_fn=format_positions,
        account_no=trader.account_no
    )


def restapi_query_fills(trader, **kw):
    return handle_pagination(
        method_name='query_fills',
        content_name='fill_list',
        pagination=kw['pagination'],
        format_fn=format_fills,
        account_no=trader.account_no
    )


def restapi_query_orders(trader, **kw):
    return handle_pagination(
        method_name='query_orders',
        content_name='order_list',
        pagination=kw['pagination'],
        format_fn=format_orders,
        account_no=trader.account_no
    )


def restapi_query_open_orders(trader, **kw):
    return handle_pagination(
        method_name='query_open_orders',
        content_name='order_list',
        pagination=kw['pagination'],
        format_fn=format_orders,
        account_no=trader.account_no
    )


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


rest_settings = RestSettings()
rest_settings.reload()

if __name__ == '__main__':
    order = Order()
    order.code = '002230'
    order.exchange = 2  # 上海1，深圳2
    order.original_id = '10'  # 递增
    order.order_type = 1
    order.price = '16'
    order.quantity = 300
    order.side = dtp_type.ORDER_SIDE_BUY
