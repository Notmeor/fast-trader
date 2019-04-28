
import datetime
import math
import functools

from fast_trader.settings import settings


class _IDPool:
    """
    为每个不同的trader与strategy实例组合分配不同的id段
    
    完整的id段[0, max_int], 先按照strategy划分，每个srategy对应
    的id段再按trader划分
    
    能够以这种方式预分配order_id的前提是，dtp接收的order_original_id
    只须保证唯一性，而不要求严格递增。现在只有证券交易部分满足这一条件。
    对于期货交易，order_original_id须严格递增（但ctp中，存在session_id设计)
    """
    def __init__(self, max_int=2147483647,
                 max_strategies=10,
                 max_traders_per_strategy=10):
        self.max_int = max_int
        self.max_strategies = max_strategies
        self.max_traders_per_strategy = max_traders_per_strategy

        self.trader_ranges = {}
        self.trader_reserves = {}
        self.strategy_reserves = {}
        self.slice()

    def time_trim(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            def trim(rng):
                rng_len = len(rng)
                now = datetime.datetime.now()
                midnight = datetime.datetime(*now.timetuple()[:3])
                checkpoint = (now - midnight).seconds / 86400
                expired = math.ceil(checkpoint * rng_len)
                return rng[expired+1:]
            ret = func(*args, **kw)
            if isinstance(ret, range):
                return trim(ret)
            elif isinstance(ret, dict):
                return {k: trim(v) for k, v in ret.items()}
            else:
                raise TypeError
        return wrapper

    @staticmethod
    def slice_range(rng, n):
        # reserve some values
        reserve_cnt = 1000 + len(rng) % n
        new_rng = rng[:-reserve_cnt]
        reserve = rng[-reserve_cnt:]
        range_len = int(len(new_rng) / n)

        if range_len < reserve_cnt:
            raise ValueError('Range too narrow')

        ranges = []
        i = 0
        for _ in new_rng[::range_len]:
            j = i + range_len
            ranges.append(new_rng[i:j])
            i = j

        return ranges, reserve

    def slice(self):

        strategy_ranges, sys_reserve = self.slice_range(
            range(1, self.max_int + 1), self.max_strategies)

        self.strategy_ranges = {i: v for i, v in enumerate(strategy_ranges)}
        self.sys_reserve = sys_reserve

    def get_trader_ranges_and_reserves(self, strategy_id):
        strategy_range = self.strategy_ranges[strategy_id]

        ranges, reserve = self.slice_range(
            strategy_range, self.max_traders_per_strategy)

        trader_ranges = {(strategy_id, i): v[:-1000]
                         for i, v in enumerate(ranges)}
        trader_reserves = {(strategy_id, i): v[-1000:]
                           for i, v in enumerate(ranges)}

        self.trader_ranges.update(trader_ranges)
        self.trader_reserves.update(trader_reserves)
        self.strategy_reserves[strategy_id] = reserve

        return trader_ranges, trader_reserves

    def get_strategy_whole_range(self, strategy_id):
        return self.strategy_ranges[strategy_id]

    @time_trim
    def get_strategy_range_per_trader(self, strategy_id, trader_id):
        if (strategy_id, trader_id) not in self.trader_ranges:
            self.get_trader_ranges_and_reserves(strategy_id)
        return self.trader_ranges[strategy_id, trader_id]

    @time_trim
    def get_strategy_reserve_per_trader(self, strategy_id, trader_id):
        if (strategy_id, trader_id) not in self.trader_ranges:
            self.get_trader_ranges_and_reserves(strategy_id)
        return self.trader_reserves[strategy_id, trader_id]

    @time_trim
    def get_strategy_reserve(self, strategy_id):
        if strategy_id not in self.strategy_reserves:
            self.get_trader_ranges_and_reserves(strategy_id)
        return self.strategy_reserves[strategy_id]

    @time_trim
    def get_sys_reserve(self):
        return self.sys_reserve

_id_pool = _IDPool(
    max_strategies=settings['_IDPool']['max_strategies'],
    max_traders_per_strategy=settings['_IDPool']['max_traders_per_strategy']
)
