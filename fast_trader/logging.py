# -*- coding: utf-8 -*-

import os
import logging.config

from fast_trader.settings import settings


def setup_logging():
    try:
        logging.config.dictConfig(settings['logging'])
    except:
        logging.basicConfig(level=logging.INFO)
