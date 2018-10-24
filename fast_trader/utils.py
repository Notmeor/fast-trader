# -*- coding: utf-8 -*-

import time, datetime
import functools

from google.protobuf.message import Message
from google.protobuf.pyext._message import RepeatedCompositeContainer
import yaml


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print('%s in %.6f secs' % (
            func.__name__, time.time() - t0_))
        return ret
    return wrapper


def load_config(path='./config.yaml'):
    with open(path, 'r') as f:
        conf = yaml.load(f)
    return conf


def message2dict(msg):
    """
    Convert protobuf message to dict
    """
    dct = {}
    if isinstance(msg, Message):
        fields = msg.ListFields()

        for field, value in fields:
            dct[field.name] = message2dict(value)
        return dct
    elif isinstance(msg, RepeatedCompositeContainer):
        return list(map(message2dict, msg))
    else:
        return msg
