

from fast_trader.strategy import dtp_type
from fast_trader.strategy import Strategy, StrategyFactory


class DemoStrategy(Strategy):
    """
    测试卖出逆回购
    """

    strategy_id = 41
    strategy_name = '测试卖出逆回购'

    def on_start(self):
        """
        响应策略启动
        """
        self.last_order = self.insert_order(
            code='131810', 
            price=14,
            quantity=100,
            order_side=dtp_type.ORDER_SIDE_REVERSE_REPO,
            exchange=dtp_type.EXCHANGE_SZ_A)

    def on_order(self, order):
        """
        响应报单回报
        """
        print('\n-----报单回报-----')
        print(order)

    def on_trade(self, trade):
        """
        响应成交回报
        """
        print('\n-----成交回报-----')
        print(trade)

        if self.last_order.status == dtp_type.ORDER_STATUS_PARTIAL_FILLED:
            self.cancel_order(**self.last_order)

    def on_order_cancelation(self, data):
        """
        响应撤单回报
        """
        print('\n-----撤单回报-----')
        print(data)


if __name__ == '__main__':

    factory = StrategyFactory()
    strategy = factory.generate_strategy(
        DemoStrategy,
        strategy_id=DemoStrategy.strategy_id,
        account_no='011000106328',
    )

    strategy.start()
