# -*- coding: utf-8 -*-

import sys
import functools
import getpass
import json
import requests

import inflection

from fast_trader.settings import settings
from fast_trader.utils import get_local_ip, AnnotationCheckMixin



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


def get_user_id():
    print('Dummy user id returned. Only for test use!')
    return 'cfdbed2e-5cfb-4979-9ea3-ac8fff05a795'


default_headers = {
    'Content-Type': 'application/json; charset=utf8',
    'userId': get_user_id(),  # FIXME
    'ip': user_meta.get('ip', get_local_ip()),
    'mac': user_meta['mac'],
    'harddisk': user_meta['harddisk'].strip(),
    'token': user_meta['token'],
}


def request(url, headers, body, method='post'):
    data = json.dumps(body)
    meth = getattr(session, method)
    r = meth(url, headers=headers, data=data)
    if r.status_code not in [200, 201]:
        raise requests.HTTPError(f'{r.status_code}')
    if r.text:
        return json.loads(r.text)
    return r.text


def get_account():
    url = settings['rest_api']['get_account'].format(
        account_no=settings['account'])
    headers = {
        'Content-Type': default_headers['Content-Type'],
        'userId': default_headers['userId']
    }
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


def cancel_order(order_original_id, order_exchange_id):
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


def cancel_batch_order():
    # TODO: 無效。全撤？
    url = settings['rest_api']['cancel_batch'].format(
        account_no=settings['account'])
    headers = default_headers
    body = {}
    return request(url, headers=headers, body=body)


def _login(user, passwd, *args, **kw):
    print(user, passwd)

def restapi_login(trader, account, password, *args, **kw):
    stats = get_account()[0]
    print(account == stats['cashAccountNo'], account, stats['cashAccountNo'])
    if stats['loginStatus'] == 1:
        trader._logined = True
        trader._token = user_meta['token']
        print('Login success')
    else:
        print('Login failed')
    

def restapi_place_order():
    pass


def restapi_cancel_order():
    pass


def restapi_place_batch_order():
    pass


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
      
