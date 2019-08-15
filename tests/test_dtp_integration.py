

from fast_trader.dtp_trade import dtp_api, DTP, Dispatcher, dtp_type, Trader


class TestTrader:

    def __init__(self):
        self.dispatcher = Dispatcher()
        self.trade_api = DTP(self.dispatcher)
        self.trader = Trader(
            dispatcher=self.dispatcher,
            trade_api=self.trade_api,
            trader_id=0)
        self.trader.set_account_no('011000106328')

    def test_get_capital(self):
        capital = self.trader.query_capital()
        print('Capital: ', capital)

    def test_get_orders(self):
        orders = self.trader.query_orders()['body']
        print('order count: ', len(orders))

    def test_get_trades(self):
        trades = self.trader.query_trades()['body']
        print('trade count: ', len(trades))

    def test_get_positions(self):
        positions = self.trader.query_positions()['body']
        print('pos count: ', len(positions))

    def test_place_order(self):
        order = {
            'account_no': '011000106328',
            'code': '002092',
            'exchange': 2,
            'order_original_id': '980',
            'price': '7.9',
            'quantity': 700,
            'order_type': 1,
            'order_side': 1,
        }
        return self.trader.place_order(**order)

    def test_place_batch_order(self):
        orders = []
        order = {
            'account_no': '011000106328',
            'code': '002092',
            'exchange': 2,
            'order_original_id': '1211',
            'price': '7.91',
            'quantity': 700,
            'order_type': 1,
            'order_side': 1,
        }
        orders.append(order)
        
        order = {
            'account_no': '011000106328',
            'code': '002092',
            'exchange': 2,
            'order_original_id': '1211',
            'price': '7.92',
            'quantity': 700,
            'order_type': 1,
            'order_side': 1,
        }
        orders.append(order)

        return self.trader.place_batch_order(orders)
    
    def run_all(self):
        self.test_get_capital()
        self.test_get_orders()
        self.test_get_trades()
        self.test_get_positions()

if __name__ == '__main__':

    test = TestTrader()

    test.run_all()
#    test.test_place_order()
#    test.test_place_batch_order()