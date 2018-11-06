# -*- coding: utf-8 -*-

"""
策略历史仓位记录
"""


class PositionStore(object):

    def get_position(self, strategy_id):
        """
        获取策略历史持仓

        Returns
        ----------
        ret: list
            [
                {
                    'exchange': 2,
                    'code': '000001',
                    'cost': 10.00,
                    'yesterday_long_quantity': 100,
                    'quantity': 200
                },
                ...
            ]
        """
        raise NotImplementedError

    def update_position(self, strategy_id):
        raise NotImplementedError


class SqlitePositionStore(PositionStore):

    pass