# -*- coding: utf-8 -*-

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import String, Column, Integer, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base

from fast_trader.app.models import StrategyLogModel


Base = declarative_base()

settings = {
    'sqlalchemy_url': 'mysql+pymysql://root:idwzx.com@192.168.211.190:3306/'
                      'dtp_trade_test?charset=utf8',
    'strategy_host': '127.0.0.1',
    'strategy_port': 5600,
    'strategy_directory': '/Users/eisenheim/Documents/git/fast-trader/'
                          'fast_trader/app',
}


engine = None
Session = None


def config_sqlalchemy():
    global engine
    global Session
    engine = create_engine(settings['strategy_channel'])
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)


class SqlLogHandler(logging.Handler):

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

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
