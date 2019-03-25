# -*- coding: utf-8 -*-

from sqlalchemy import String, Column, Integer, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BaseDocument:

    @classmethod
    def from_msg(cls, msg):
        ret = cls()
        for k, v in msg.items():
            setattr(ret, k, v)
        return ret


class PositionModel(Base, BaseDocument):
    __tablename__ = 'strategy_position'

    id = Column(Integer, primary_key=True)

    update_date = Column(String(10))
    update_time = Column(String(16))

    code = Column(String(10))
    name = Column(String(40))
    # 剩余持仓
    balance = Column(Integer)
    # 当日可交易持仓数量
    available_quantity = Column(Integer)
    # 冻结数量
    freeze_quantitty = Column(Integer)
    # 当日买入数量
    buy_quantity = Column(Integer)
    # 当日卖出数量
    sell_quantity = Column(Integer)
    # 最新市值
    market_value = Column(String(26))
    # 持仓均价
    cost = Column(String(26))


class TradeModel(Base, BaseDocument):
    __tablename__ = 'strategy_trade'

    update_date = Column(String(10))
    update_time = Column(String(16))

    # 交易所成交编号
    fill_exchange_id = Column(String(40), primary_key=True)
    # 成交的时间
    fill_time = Column(String(12))
    # 成交状态 0:未知 1:成交 2:撤单 3: 废单 4:确认
    # (TBD: 可能来自'撤销标志') *** 只有撤单和成交两个状态
    fill_status = Column(Integer)
    # 本次成交价格
    fill_price = Column(String(26))
    # 本次成交数量; fill_status为撤单时，此数值为撤单数量
    fill_quantity = Column(Integer)
    # 本次成交金额
    fill_amount = Column(Integer)
    # 本次清算资金(委托为卖出方向时表示本次成交新增的可用资金)
    clear_amount = Column(String(26))
    # 交易所委托号
    order_exchange_id = Column(String(20))
    # 客户委托号
    order_original_id = Column(String(20))
    # 交易所
    exchange = Column(Integer)
    code = Column(String(10))
    name = Column(String(40))
    # 成交方向
    order_side = Column(Integer)


class CapitalModel(Base, BaseDocument):
    __tablename__ = 'strategy_capital'

    id = Column(Integer, primary_key=True)

    update_date = Column(String(10))
    update_time = Column(String(16))

    account_no = Column(String(20))
    # 账户余额
    balance = Column(String(26))
    # 可用资金
    available = Column(String(26))
    # 冻结资金
    freeze = Column(String(26))
    # 证券市值
    securities = Column(String(26))
    # 总资产
    total = Column(String(26))


class StrategyLogModel(Base, BaseDocument):
    __tablename__ = 'strategy_log'

    id = Column(Integer, primary_key=True)

    msg = Column(String(5000))


class StrategyStatus(Base, BaseDocument):
    __tablename__ = 'srategy_status'

    id = Column(Integer, primary_key=True)
    # 策略进程id
    pid = Column(Integer)
    # 账户号
    account_no = Column(String(30))
    # token
    token = Column(String(10))
    # 策略id
    strategy_id = Column(Integer)
    # 是否运行中
    running = Column(Boolean)
    # 启动时间
    start_time = Column(String(30))
    # 最新心跳时间戳
    last_heartbeat = Column(Integer)
