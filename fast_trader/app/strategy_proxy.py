# -*- coding: utf-8 -*-

from fast_trader.utils import timeit, message2dict, int2datetime, attrdict

import os
import zmq
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fast_trader.app.settings import settings, Session
from fast_trader.app.models import StrategyStatus



class StrategyProxy:

    def __init__(self, strategy_id):
        self._sock = zmq.Context().socket(zmq.REQ)
        self._sock.setsockopt(zmq.REQ_RELAXED, 1)
        self._sock.connect(settings['strategy_channel'])

        self.strategy_id = strategy_id
        self._status = {}
        self.token = ''
        # self._running = False

    def _retrieve_status(self):
        session = Session()
        status = (
            session
            .query(StrategyStatus)
            .filter_by(strategy_id=self.strategy_id)
            .one()
        )
        self._status = status

    def is_running(self):
        return self._status.get('running', False)

    def send_request(self, request):
        self._sock.send_json(request)
        ret = self._sock.recv_json()
        return ret

    def start_strategy(self):
        """
        Returns
        ----------
        ret: bool
            策略启动成功返回`True`, 否则返回`False`
        """
        if self.is_running():
            return True
        rsp = self.send_request({
            'strategy_id': self.strategy_id,
            'api_name': 'start_strategy',
            'kw': {},
        })
        if rsp['ret_code'] == 0:
            self.token = rsp['data']['token']
            self._status['running'] = True
        return self.is_running()

    def stop_strategy(self):
        return self.send_request({
            'strategy_id': self.strategy_id,
            'api_name': 'stop_strategy',
            'kw': {},
        })

    def buy(self, code, price, quantity):
        return self.send_request({
            'strategy_id': self.strategy_id,
            'token': self.token,
            'api_name': 'buy',
            'kw': {
                'code': code,
                'price': price,
                'quantity': quantity,
            },
        })

    def sell(self, code, price, quantity):
        return self.send_request({
            'strategy_id': self.strategy_id,
            'token': self.token,
            'api_name': 'sell',
            'kw': {
                'code': code,
                'price': price,
                'quantity': quantity,
            },
        })

    def get_capital(self):
        return self.send_request({
            'strategy_id': self.strategy_id,
            'token': self.token,
            'api_name': 'get_capital',
            'kw': {},
        })

    def get_positions(self):
        return self.send_request({
            'strategy_id': self.strategy_id,
            'token': self.token,
            'api_name': 'get_account_positions',
            'kw': {},
        })

    def get_trades(self):
        return self.send_request({
            'strategy_id': self.strategy_id,
            'token': self.token,
            'api_name': 'get_trades',
            'kw': {},
        })

    def get_open_orders(self):
        return self.send_request({
            'strategy_id': self.strategy_id,
            'token': self.token,
            'api_name': 'get_open_orders',
            'kw': {},
        })