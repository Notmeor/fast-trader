# -*- coding: utf-8 -*-

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import os

os.environ['FAST_TRADER_CONFIG'] = os.path.join(
    os.path.dirname(__file__), 'config.yaml')

from fast_trader.settings import settings
from fast_trader.app.models import StrategyLogModel, Base


engine = None
Session = None


def config_sqlalchemy():
    global engine
    global Session
    engine = create_engine(
        settings['batch_order_dealer_app']['sqlalchemy_url'])
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)


config_sqlalchemy()


class SqlLogHandler(logging.Handler):

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.setFormatter(formatter)

    def emit(self, record):
        try:
            msg = self.format(record)
            session = Session()

            item = StrategyLogModel()
            item.msg = msg

            session.add(item)
            session.commit()
        except Exception:
            self.handleError(record)
