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
import psutil

import sqlalchemy.orm.exc as orm_exc

from fast_trader.settings import settings, Session, SqlLogHandler
from fast_trader.strategy import Strategy, StrategyFactory
from fast_trader.models import StrategyStatus, StrategyServerModel
from fast_trader.utils import timeit
from fast_trader.rest_api import user_meta


SERVER_TIMEOUT_SECS = 3


class StrategyNotFound(Exception):
    pass


def get_current_ts():
    return int(datetime.datetime.now().timestamp())


class Manager:

    def __init__(self):
        self._sock = zmq.Context().socket(zmq.REP)
        # self._sock.setsockopt(zmq.RCVTIMEO, 2000)
        host = settings['app']['strategy_host']
        port = settings['app']['strategy_port']
        conn = f"tcp://{host}:{port}"
        self._sock.bind(conn)
        
        self._update_server_status()

        self._load_strategy_settings()
        self._factory = None
        self._strategies = {}

        self._heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        
    def _load_strategy_settings(self):
        if settings['use_rest_api'] is True:
            self._strategy_settings = user_meta.copy()
            self._strategy_settings.pop('token')
        else:
            self._strategy_settings = None
    
    def _update_server_status(self):
        now = datetime.datetime.now()
        now_str = now.strftime(
                '%Y%m%d %H:%M:%S.%f')
        ts = get_current_ts()
        msg = {
            'pid': os.getpid(),
            'start_time': now_str,
            'last_heartbeat': ts
        }
        session = Session()
        last_status = (
            session
            .query(StrategyServer)
            .filter_by(id=1)
            .first()
        )
        if last_status is None:
            status = StrategyServer.from_msg(msg)
            session.add(status)
        else:
            for k, v in msg.items():
                setattr(last_status, k, v)

        session.commit()
        session.close()

    def receive(self):
        return self._sock.recv_json()  # zmq.NOBLOCK)

    def send(self, msg):
        self._sock.send_json(msg)

    @timeit
    def send_heartbeat(self):
        session = Session()
        while True:
            time.sleep(1)
            ts = get_current_ts()
            
            (session
                 .query(StrategyServer)
                 .filter_by(id=1)
                 .update({'last_heartbeat': ts}))
                            
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
            if strategy_id in self._strategies:
                return {'ret_code': -1, 'err_msg': 'Already running'}

            strategy = self.instantiate_strategy(strategy_id)

            ret = strategy.start()

            if ret['ret_code'] != 0:
                strategy.remove_self()
                return ret

            data = {
                'pid': os.getpid(),
                'account_no': strategy.trader.account_no,
                'strategy_id': strategy.strategy_id,
                'strategy_name': strategy.strategy_name,
                'start_time': datetime.datetime.now(
                    ).strftime('%Y%m%d %H:%M:%S.%f'),
                'token': strategy.trader._token,
                'running': True}

            self._update_strategy_status(data)
            self.add_strategy(strategy)

            return {'ret_code': 0, 'data': data}

        except Exception as e:
            return {'ret_code': -1, 'err_msg': repr(e)}

    def stop_strategy(self, strategy_id):
        try:
            strategy = self.get_strategy(strategy_id)
            strategy.remove_self()
            self._strategies.pop(strategy_id)
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
        session.close()

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
        ss = StrategyLoader().load()
        try:
            StrategyCls = next(
                filter(lambda x: x.strategy_id == strategy_id, ss))
        except StopIteration:
            raise RuntimeError(f'策略读取失败，strategy_id: {strategy_id}')

        strategy = self.factory.generate_strategy(
            StrategyCls,
            trader_id=1,
            strategy_id=strategy_id
        )
        return strategy

    def run(self):
        logger = logging.getLogger('strategy_mananger')
        logger.addHandler(SqlLogHandler())
        logger.info(f'Strategy manager started. Pid={os.getpid()}')

        self._heartbeat_thread.start()

        while True:
            # 监听外部指令
            try:
                request = self.receive()
            except zmq.Again:
                pass
            else:
                logger.info(f'received: {request}')
                ret = self.handle_request(request)
                self.send(ret)
                logger.info(f'sent: {ret}')


class StrategyLoader:

    def __init__(self):
        self.strategy_suffix = '.py'
        self.strategy_dir = \
            settings['app']['strategy_directory']

    def load(self):
        strategy_classes = []

        if self.strategy_dir == '':
            strategy_dir = os.path.dirname(__file__)
        else:
            strategy_dir = self.strategy_dir

        for fl in os.listdir(strategy_dir):
            if not fl.endswith(self.strategy_suffix):
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
    proc = subprocess.Popen(
        ['python', __file__],
        shell=False,
        bufsize=1,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc
    return proc.pid


def stop_strategy_server():
    session = Session()
    pid = session.query(StrategyServer.pid).one()[0]
    for proc in psutil.process_iter():
        if proc.pid == pid:
            proc.kill()
            break    


class StrategyServer:
    
    def __init__(self):
        self.server_id = 1
        self.proc = None
    
    def start(self):
        proc = self.proc = subprocess.Popen(
            ['python', __file__],
            shell=False,
            bufsize=1,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return proc

    def stop(self):
        session = Session()
        pid = session.query(StrategyServerModel.pid).one()[0]
        for proc in psutil.process_iter():
            if proc.pid == pid:
                proc.kill()
                break  
        session.close()
    
    def is_running(self):
        session = Session()
        last_ts = (
            session
            .query(StrategyServerModel.last_heartbeat)
            .filter_by(id=self.server_id)
            .one()[0])
        
        current_ts = get_current_ts()
        if current_ts > last_ts + SERVER_TIMEOUT_SECS:
            return False
        return True
            

    

#import subprocess, datetime, time, sys
#
#def foo():
#    while True:
#        print('stdout:'+str(datetime.datetime.now()), flush=True)
#        print('stderr:'+str(datetime.datetime.now()), file=sys.stderr, flush=True)
#        time.sleep(1.5)
#
#def main():
#    foo()


if __name__ == '__main__':
    # Do not edit!
    main()

