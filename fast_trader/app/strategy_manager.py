# -*- coding: utf-8 -*-

import os

import datetime
import time
import zmq
import logging
import importlib
import threading
import subprocess
import multiprocessing

import sqlalchemy.orm.exc as orm_exc

from fast_trader.app.settings import settings
from fast_trader.strategy import Strategy, StrategyFactory
from fast_trader.app.settings import Session, SqlLogHandler
from fast_trader.app.models import StrategyStatus


class StrategyNotFound(Exception):
    pass


class Manager:

    def __init__(self):
        self._sock = zmq.Context().socket(zmq.REP)
        host = settings['batch_order_dealer_app']['strategy_host']
        port = settings['batch_order_dealer_app']['strategy_port']
        conn = f"tcp://{host}:{port}"
        self._sock.bind(conn)

        self._strategy_settings = None
        self._factory = None
        self._strategies = {}

        self._heartbeat_thread = threading.Thread(target=self.send_heartbeat)

    def receive(self):
        return self._sock.recv_json()  # (zmq.NOBLOCK)

    def send(self, msg):
        self._sock.send_json(msg)

    def send_heartbeat(self):
        session = Session()
        while True:
            time.sleep(1)
            ts = int(datetime.datetime.now().timestamp())
            for _id in self._strategies:
                (session
                 .query(StrategyStatus)
                 .filter_by(strategy_id=_id)
                 .update({'last_heartbeat': ts}))
            session.commit()

    @property
    def factory(self):
        if self._strategy_settings is None:
            raise Exception('交易参数未配置')
        if self._factory is None:
            self._factory = StrategyFactory(
                factory_settings=self._strategy_settings)
            self._factory.dtp.logger.addHandler(SqlLogHandler())
        return self._factory

    def update_settings(self, strategy_settings):
        self._strategy_settings = strategy_settings
        return {'ret_code': 0, 'data': None}

    def add_strategy(self, strategy):
        self._strategies[strategy.strategy_id] = strategy

    def get_strategy(self, strategy_id):
        try:
            return self._strategies[strategy_id]
        except KeyError:
            raise StrategyNotFound('策略未启动')

    def start_strategy(self, strategy_id):
        try:
            if strategy_id not in self._strategies:
                self.instantiate_strategy(strategy_id)

            strategy = self.get_strategy(strategy_id)

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
            return {'ret_code': -1, 'err_msg': repr(e)}

    def stop_strategy(self, strategy_id):
        try:
            strategy = self.get_strategy(strategy_id)
            self.factory.remove_strategy(strategy)
            return {'ret_code': 0, 'data': None}
        except Exception as e:
            return {'ret_code': -1, 'err_msg': repr(e)}

    def _operate(self, request):

        try:
            strategy = self.get_strategy(request['strategy_id'])
            if request['token'] != strategy.trader._token:
                return {'ret_code': -1, 'err_msg': 'token错误'}

            ret = getattr(strategy, request['api_name'])(**request['kw'])
            return {'ret_code': 0, 'data': ret}

        except Exception as e:
            return {'ret_code': -1, 'err_msg': repr(e)}

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
        # strategy = self._strategies[request['strategy_id']]

        if request['api_name'] == 'start_strategy':
            return self.start_strategy(request['strategy_id'])
        elif request['api_name'] == 'stop_strategy':
            return self.stop_strategy(request['strategy_id'])
        elif request['api_name'] == 'update_settings':
            return self.update_settings(request['settings'])
        else:
            return self._operate(request)

    def instantiate_strategy(self, strategy_id):
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
        logger.info('Strategy manager started...')

        self._heartbeat_thread.start()

        while True:
            # 监听外部指令
            try:
                request = self.receive()
            except zmq.Again:
                self.send_heartbeat()
                time.sleep(0.2)
            else:
                logger.info(f'received: {request}')
                ret = self.handle_request(request)
                self.send(ret)
                logger.info(f'sent: {ret}')


class StrategyLoader:

    def __init__(self):
        self.strategy_dir = \
            settings['batch_order_dealer_app']['strategy_directory']

    def load(self):
        strategy_classes = []

        if self.strategy_dir == '':
            strategy_dir = os.path.dirname(__file__)
        else:
            strategy_dir = self.strategy_dir

        for fl in os.listdir(strategy_dir):
            if not fl.endswith('ler.py'):
                continue
            path = os.path.join(strategy_dir, fl)
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


def main():
    manager = Manager()
    manager.run()


def start_strategy_server():
    proc = subprocess.Popen(['python', __file__])
    return proc.pid


if __name__ == '__main__':

    main()
