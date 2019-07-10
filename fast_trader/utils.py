# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime
import functools
import math

from google.protobuf.message import Message
from google.protobuf.pyext._message import RepeatedCompositeContainer
from google.protobuf.pyext._message import RepeatedScalarContainer

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
    
    # TODO: profile

    __slots__ = ()
    
    def __init__(self, *args, **kw):
        dict.__init__(self, *args, **kw)
        for k in self:
            if isinstance(self[k], dict):
                self[k] = attrdict(self[k])

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

        if 'handler_id' not in kw:
            kw['handler_id'] = f'{api_id}_{api_type}'

        if 'sync' not in kw:
            kw['sync'] = False

        if 'ret_code' not in kw:
            kw['ret_code'] = 0

        kw.update({
            'api_type': api_type,
            'api_id': api_id
        })

        self.update(kw)

#    @property
#    def handler_id(self):
#        if 'handler_id' in self:
#            return self['handler_id']
#        handler_id = f'{self["api_id"]}_{self["api_type"]}'
#        return handler_id


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


def get_win_user_documents_dir():
    if sys.platform != 'win32':
        raise RuntimeError('Not on Windows!')

    import ctypes
    from ctypes.wintypes import MAX_PATH
    dll = ctypes.windll.shell32
    buf = ctypes.create_unicode_buffer(MAX_PATH + 1)
    if dll.SHGetSpecialFolderPathW(None, buf, 0x0005, False):
        return buf.value
    else:
        raise RuntimeError('Failed to retrieve `Documents` path!')


def as_wind_code(code, exchange=None):
    if '.' in code:
        return code
    if exchange is None:
        if code.startswith('6'):
            return code + '.SH'
        else:
            return code + '.SZ'
    else:
        # 根据交易所添加相应后缀
        raise NotImplementedError


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
def camel2snake(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def get_current_ts():
    return int(datetime.datetime.now().timestamp())


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
