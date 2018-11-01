# -*- coding: utf-8 -*-

import os
import time, datetime
import functools

from google.protobuf.message import Message
from google.protobuf.pyext._message import RepeatedCompositeContainer
import yaml


class Mail(object):

    def __init__(self, api_id, api_type, **kw):

        if 'handler_id' not in kw:
            kw['handler_id'] = '{}_{}'.format(api_id, api_type)

        if 'sync' not in kw:
            kw['sync'] = False

        if 'ret_code' not in kw:
            kw['ret_code'] = 0

        kw.update({
            'api_id': api_id,
            'api_type': api_type
        })

        self._kw = kw

    def __getitem__(self, key):
        return self._kw[key]

    def __setitem__(self, key, value):
        self._kw[key] = value

    def __getattr__(self, key):
        return self._kw[key]

    def __setattr__(self, key, value):
        if key != '_kw':
            raise AttributeError('Assignment not allowed')
        super().__setattr__(key, value)

    def __repr__(self):
        return repr(self._kw)

    def get(self, key, default=None):
        return self._kw.get(key, default)


class Foo(object):

    def __init__(self, **kw):
        self._kw = kw

    def __getitem__(self, key):
        return self._kw[key]

    def __setitem__(self, key, value):
        self._kw[key] = value

    def __getattr__(self, key):
        return self._kw[key]

    def __setattr__(self, key, value):
        if key != '_kw':
            raise AttributeError('Assignment not allowed')
        super().__setattr__(key, value)

    def __repr__(self):
        return repr(self._kw)

    def get(self, key, default=None):
        return self._kw.get(key, default)


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print('%s in %.6f secs' % (
            func.__name__, time.time() - t0_))
        return ret
    return wrapper


def load_config(path=None):
    if path is None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.yaml')
    with open(path, 'r') as f:
        conf = yaml.load(f)
    return conf


def message2dict_(msg, including_default_value_fields=True):
    """
    Convert protobuf message to dict
    """

    dct = {}

    if isinstance(msg, Message):

        if including_default_value_fields:
            for field in msg.DESCRIPTOR.fields:
                dct[field.name] = field.default_value

        fields = msg.ListFields()
        for field, value in fields:
            dct[field.name] = message2dict(value)

        return dct

    elif isinstance(msg, RepeatedCompositeContainer):
        return list(map(message2dict, msg))

    else:
        return msg


@timeit
def message2dict(msg, including_default_value_fields=True):
    """
    Convert protobuf message to dict
    """
    # return msg

    dct = {}

    if isinstance(msg, Message):

        for field in msg.DESCRIPTOR.fields:
            name = field.name
            dct[name] = message2dict(getattr(msg, name))

        return dct

    elif isinstance(msg, RepeatedCompositeContainer):
        return list(map(message2dict, msg))

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


def _convert(ss):
    import re
    return '_'.join(re.findall('[A-Z][^A-Z]*', ss)).upper()