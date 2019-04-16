# -*- coding: utf-8 -*-

import os
import time
import datetime
import functools
import math

from google.protobuf.message import Message
from google.protobuf.pyext._message import RepeatedCompositeContainer
from google.protobuf.pyext._message import RepeatedScalarContainer
import yaml
import uuid
import socket
import re


class AnnotationCheckMixin:
    """
    Data members must be of the same number and types declared
    in the annoation.
    
    NOTE: `typing` type annotation is not supported
    """
    def _check_fields(self):
        if hasattr(self, '__slots__'):
            attrs = self.__slots__
        else:
            attrs = self.__dict__.keys()
        
        unexpected_fields = set(attrs).difference(self.__annotations__)
        if unexpected_fields:
            raise RuntimeError(f'Unexpected fields: {unexpected_fields}')

        for k in self.__annotations__:
            if k not in attrs:
                raise RuntimeError(f'Missing field: `{k}`')

            _type = type(getattr(self, k))
            dst_type = self.__annotations__[k]

            if _type is not dst_type:
                raise TypeError(
                    f'Expect type {dst_type} for `{k}`, got {_type}')


class attrdict(dict):

    __slots__ = ()

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        raise AttributeError('Assignment not allowed')
    
    def copy(self):
        return attrdict(self)


class Mail(attrdict):

    def __init__(self, api_type, api_id, **kw):

        if api_type == 'req':
            kw['request_id'] = str(uuid.uuid1())

        if 'handler_id' not in kw:
            kw['handler_id'] = '{}_{}'.format(api_id, api_type)

        if 'sync' not in kw:
            kw['sync'] = False

        if 'ret_code' not in kw:
            kw['ret_code'] = 0

        kw.update({
            'api_type': api_type,
            'api_id': api_id
        })

        self.update(kw)


def get_mac_address():
    mac = uuid.getnode()
    ret = ':'.join(re.findall('..', '%012x' % mac)).upper()
    return ret


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
def camel2snake(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print('Finished %s in %.6f secs' % (
            func.__name__, time.time() - t0_))
        return ret
    return wrapper


def message2dict(msg, including_default_value_fields=True):
    """
    Convert protobuf message to dict
    """

    dct = attrdict()

    if isinstance(msg, Message):

        for field in msg.DESCRIPTOR.fields:
            name = field.name
            dct[name] = message2dict(getattr(msg, name))

        return dct

    elif isinstance(msg, RepeatedCompositeContainer):
        return list(map(message2dict, msg))

    elif isinstance(msg, RepeatedScalarContainer):
        return list(msg)

    else:
        return msg


def message2tuple(msg, kind):
    """
    Convert protobuf message to namedtuple
    Doesn't support nested messages
    """

    dct = {}

    for field in msg.DESCRIPTOR.fields:
        name = field.name
        dct[name] = getattr(msg, name)

    ret = kind(**dct)

    return ret


def int2datetime(n_date=None, n_time=None, utc=False):
    if n_date is None and n_time is None:
        raise ValueError
    elif n_date and n_time is None:
        dt = datetime.datetime.strptime('{}'.format(n_date), '%Y%m%d')
    elif n_date is None and n_time:
        dt = datetime.datetime.strptime('{}'.format(n_time), '%H%M%S%f').time()
    else:
        dt = datetime.datetime.strptime(
            '{}{}'.format(n_date, n_time),
            '%Y%m%d%H%M%S%f')
    if utc:
        return dt.astimezone(datetime.timezone.utc)
    return dt


class _IDPool:
    """
    为每个不同的trader与strategy实例组合分配不同的id段
    """
    def __init__(self, max_int=2147483647,
                 max_strategies=10,
                 max_traders_per_strategy=10):
        self.max_int = max_int
        self.max_strategies = max_strategies
        self.max_traders_per_strategy = max_traders_per_strategy

        self.trader_ranges = {}
        self.trader_reserves = {}
        self.strategy_reserves = {}
        self.slice()

    def time_trim(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            def trim(rng):
                rng_len = len(rng)
                now = datetime.datetime.now()
                midnight = datetime.datetime(*now.timetuple()[:3])
                checkpoint = (now - midnight).seconds / 86400
                expired = math.ceil(checkpoint * rng_len)
                return rng[expired+1:]
            ret = func(*args, **kw)
            if isinstance(ret, range):
                return trim(ret)
            elif isinstance(ret, dict):
                return {k: trim(v) for k, v in ret.items()}
            else:
                raise TypeError
        return wrapper

    @staticmethod
    def slice_range(rng, n):
        # reserve some values
        reserve_cnt = 1000 + len(rng) % n
        new_rng = rng[:-reserve_cnt]
        reserve = rng[-reserve_cnt:]
        range_len = int(len(new_rng) / n)

        if range_len < reserve_cnt:
            raise ValueError('Range to narrow')

        ranges = []
        i = 0
        for _ in new_rng[::range_len]:
            j = i + range_len
            ranges.append(new_rng[i:j])
            i = j

        return ranges, reserve

    def slice(self):

        strategy_ranges, sys_reserve = self.slice_range(
            range(1, self.max_int + 1), self.max_strategies)

        self.strategy_ranges = {i: v for i, v in enumerate(strategy_ranges)}
        self.sys_reserve = sys_reserve

    def get_trader_ranges_and_reserves(self, strategy_id):
        strategy_range = self.strategy_ranges[strategy_id]

        ranges, reserve = self.slice_range(
            strategy_range, self.max_traders_per_strategy)

        trader_ranges = {(strategy_id, i): v[:-1000]
                         for i, v in enumerate(ranges)}
        trader_reserves = {(strategy_id, i): v[-1000:]
                           for i, v in enumerate(ranges)}

        self.trader_ranges.update(trader_ranges)
        self.trader_reserves.update(trader_reserves)
        self.strategy_reserves[strategy_id] = reserve

        return trader_ranges, trader_reserves

    def get_strategy_whole_range(self, strategy_id):
        return self.strategy_ranges[strategy_id]

    @time_trim
    def get_strategy_range_per_trader(self, strategy_id, trader_id):
        if (strategy_id, trader_id) not in self.trader_ranges:
            self.get_trader_ranges_and_reserves(strategy_id)
        return self.trader_ranges[strategy_id, trader_id]

    @time_trim
    def get_strategy_reserve_per_trader(self, strategy_id, trader_id):
        if (strategy_id, trader_id) not in self.trader_ranges:
            self.get_trader_ranges_and_reserves(strategy_id)
        return self.trader_reserves[strategy_id, trader_id]

    @time_trim
    def get_strategy_reserve(self, strategy_id):
        if strategy_id not in self.strategy_reserves:
            self.get_trader_ranges_and_reserves(strategy_id)
        return self.strategy_reserves[strategy_id]

    @time_trim
    def get_sys_reserve(self):
        return self.sys_reserve

# TODO: configurable
_id_pool = _IDPool()
