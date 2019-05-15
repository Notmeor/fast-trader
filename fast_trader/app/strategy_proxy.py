# -*- coding: utf-8 -*-

import os
import zmq
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy.orm.exc as orm_exc

from fast_trader.settings import settings, Session
from fast_trader.models import StrategyStatus
from fast_trader.app.strategy_manager import StrategyLoader, StrategyServer

class _StatusStore:
    
    def __init__(self):
        from fast_trader.sqlite import SqliteStore
        fields = ['pid', 'account_no', 'token', 'strategy_id',
                  'strategy_name', 'running', 'start_time',
                  'last_heartbeat']
        db_name = settings['sqlalchemy_url'].split(':///')[1]
        table_name = 'strategy_status'
        self.store = SqliteStore(db_name, table_name, fields)
    
    def load_status(self, strategy_id):
        stats = self.store.read({'strategy_id': strategy_id})
        if stats:
            return stats[0]
        return {}
        
class StrategyProxy:

    def __init__(self, strategy_id, timeout=3):
        self._sock = zmq.Context().socket(zmq.REQ)
        self._sock.setsockopt(zmq.REQ_RELAXED, 1)
        self._sock.setsockopt(zmq.SNDTIMEO, 1000 * timeout)
        self._sock.setsockopt(zmq.RCVTIMEO, 1000 * timeout)

        host = settings['strategy_manager_host']
        port = settings['strategy_manager_port']
        url = f"tcp://{host}:{port}"
        self._sock.connect(url)

        self.strategy_id = strategy_id
        self._status = {}
        self.token = ''
        # self._running = False

    def _retrieve_status(self):
        try:
            session = Session()
            status = (
                session
                .query(StrategyStatus)
                .filter_by(strategy_id=self.strategy_id)
                .one()
            ).__dict__
            status.pop('_sa_instance_state')
        except orm_exc.NoResultFound:
            status = {}
        self._status = status

    def get_status(self):
        self._retrieve_status()
        return self._status

    def is_running(self):
        status = self.get_status()
        last_heartbeat = status.get('last_heartbeat', 0) or 0
        now = datetime.datetime.now().timestamp()
        # NOTE: 如果与策略服务端时间相差较大，则无法给出正确判断
        if now - last_heartbeat < 5 and status.get('running'):
            return True
        return False

    def send_request(self, request):
        try:
            self._sock.send_json(request)
            ret = self._sock.recv_json()
            return ret
        except zmq.Again:
            return {'ret_code': -1, 'err_msg': '连接超时'}

    def update_settings(self, config):
        self.send_request({
            'api_name': 'update_settings',
            'settings': config
        })

    def start_strategy(self):
        """
        Returns
        ----------
        ret: bool
            策略启动成功返回`True`, 否则返回`False`
        """
#        if self.is_running():
#            return {'ret_code': 0, 'data': '策略运行中，无需重复启动'}

        rsp = self.send_request({
            'strategy_id': self.strategy_id,
            'api_name': 'start_strategy',
            'kw': {},
        })
        if rsp['ret_code'] == 0:
            self.token = rsp['data']['token']
            self._status['running'] = True
            return {'ret_code': 0, 'data': None}
        else:
            return rsp

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


def get_strategy_list():
    loader = StrategyLoader()
    strategies = loader.load()

    ret = []
    session = Session()
    for s in strategies:
        res = (
            session
            .query(StrategyStatus)
            .filter_by(strategy_id=s.strategy_id)
            .all())
        if res:
            ea = res[0]
            ret.append({
                'strategy_name': ea.strategy_name,
                'strategy_id': ea.strategy_id,
                'running': ea.is_running(),
                'start_time': ea.start_time or ''
            })
        else:
            ret.append({
                'strategy_name': s.strategy_name,
                'strategy_id': s.strategy_id,
                'running': False,
                'start_time': ''})
    return ret



if __name__ == '__main__':

    p = StrategyProxy(6)
#    p.update_settings({
#        'ip': '192.168.211.169',
#        'mac': get_mac_address(),
#        'harddisk': '6B69DD46',
#    })
