# -*- coding: utf-8 -*-

import os
import datetime
import zmq
import logging
import importlib

from fast_trader.strategy import Strategy, StrategyFactory
from fast_trader.app.settings import Session, SqlLogHandler, settings
from fast_trader.app.models import StrategyStatus


class Manager:

    def __init__(self):
        self._sock = zmq.Context().socket(zmq.REP)
        conn = f"tcp://{settings['strategy_host']}:{settings['strategy_host']}"
        self._sock.bind(conn)

        self.factory = StrategyFactory()
        self._strategies = {}

    def receive(self):
        return self._sock.recv_json()

    def send(self, msg):
        self._sock.send_json(msg)

    def add_strategy(self, strategy):
        self._strategies[strategy.strategy_id] = strategy

    def start_strategy(self, strategy, config):
        try:
            if strategy.strategi_id not in self._strategies:
                self.instantiate_strategy(config)

            strategy.start()
            data = {
                'pid': os.getpid(),
                'account_no': strategy.trader._account,
                'strategy_id': strategy.strategy_id,
                'start_time': datetime.datetime.now(
                    ).strftime('%Y%m%d %H:%M:%S.%f'),
                'token': strategy.trader._token,
                'running': True}
            self._update_strategy_status(data)
            return {'ret_code': 0, 'data': data}
        except Exception as e:
            return {'ret_code': -1, 'err_msg': str(e)}

    def stop_strategy(self, strategy_id):
        try:
            strategy = self._strategies[strategy_id]
            self.factory.remove_strategy(strategy)
            return {'ret_code': 0, 'data': None}
        except Exception as e:
            return {'ret_code': -1, 'err_msg': str(e)}

    def _operate(self, strategy, request):
        if request['token'] != strategy.trader._token:
            return {'ret_code': -1, 'err_msg': 'token错误'}
        try:
            ret = getattr(strategy, request['api_name'])(**request['kw'])
            return {'ret_code': 0, 'data': ret}
        except Exception as e:
            return {'ret_code': -1, 'err_msg': str(e)}

    def _update_strategy_status(self, msg):
        session = Session()
        last_status = (
            session
            .query(StrategyStatus)
            .filter_by(strategy_id=msg['strategy_id'])
            .first()
        )
        if last_status is None:
            status = StrategyStatus.from_msg(msg)
            session.add(status)
        else:
            for k, v in msg.items():
                setattr(last_status, k, v)

        session.commit()

    def handle_request(self, request):
        strategy = self._strategies[request['strategy_id']]

        if request['api_name'] == 'start_strategy':
            return self.start_strategy(strategy)
        elif request['api_name'] == 'stop_strategy':
            return self.stop_strategy(strategy)
        else:
            return self._operate(strategy, request)
    
    def instantiate_strategy(self, strategy_id, config):
        StrategyCls = StrategyLoader().load()[0]

        strategy = self.factory.generate_strategy(
            StrategyCls,
            trader_id=1,
            strategy_id=strategy_id
        )
        self.add_strategy(strategy)
    
    def run(self):
        logger = logging.getLogger(f'{__name__}')
        logger.addHandler(SqlLogHandler())
        
        while True:
            # 监听外部指令
            request = self.receive()
            logger.info(f'received: {request}')
            ret = self.handle_request(request)
            self.send(ret)
            logger.info(f'sent: {ret}')


class StrategyLoader:

    def __init__(self):
        self.strategy_dir = settings['strategy_directory']

    def load(self):
        strategy_classes = []
        for fl in os.listdir(self.strategy_dir):
            if not fl.endswith('ler.py'):
                continue
            path = os.path.join(self.strategy_dir, fl)
            spec = importlib.util.spec_from_file_location(
                "strategy", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            for name in dir(mod):
                el = getattr(mod, name)
                if isinstance(el, type):
                    if issubclass(el, Strategy) and el is not Strategy:
                        strategy_classes.append(el)
        return strategy_classes


def start_strategy():
    from fast_trader.settings import settings
    from fast_trader.utils import get_mac_address
    settings.set({
        'ip': '192.168.211.169',
        'mac': get_mac_address(),
        'harddisk': '6B69DD46',
    })

    factory = StrategyFactory()
    strategy = factory.generate_strategy(
        StrategyCls,
        trader_id=1,
        strategy_id=1
    )



    return factory, strategy