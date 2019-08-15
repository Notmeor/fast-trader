# -*- coding: utf-8 -*-
import unittest

from fast_trader.dtp_trade import dtp_api, DTP, Dispatcher, dtp_type


class TestDTP(unittest.TestCase):

    def setUp(self):
        self.dtp = DTP(Dispatcher())
        self.dtp.start()

        self.account_no = '011000106328'

    def test_get_capital(self):
        self.assertTrue(
            isinstance(self.dtp.get_capital(self.account_no)[0], dict))

    def test_place_order(self):

        order_req = dtp_api.OrderReq()
        order_req.account_no = self.account_no
        order_req.code = '002230'
        order_req.order_original_id = '992'
        order_req.exchange = dtp_type.EXCHANGE_SZ_A
        order_req.order_type = dtp_type.ORDER_TYPE_LIMIT
        order_req.order_side = dtp_type.ORDER_SIDE_BUY
        order_req.price = '31.05'
        order_req.quantity = 700

        self.dtp.place_order(order_req)

    def test_cancel_order(self):

        order_cancelation_req = dtp_api.OrderCancelationReq()
        order_cancelation_req.account_no = self.account_no
        order_cancelation_req.exchange = dtp_type.EXCHANGE_SZ_A
        order_cancelation_req.order_exchange_id = '88'

        self.dtp.cancel_order(order_cancelation_req)

    def test_place_batch_order(self):

        batch_order_req = []

        order_req = dtp_api.OrderReq()
        batch_order_req.append(order_req)
        order_req.account_no = self.account_no
        order_req.code = '002230'
        order_req.order_original_id = '992'
        order_req.exchange = dtp_type.EXCHANGE_SZ_A
        order_req.order_type = dtp_type.ORDER_TYPE_LIMIT
        order_req.order_side = dtp_type.ORDER_SIDE_BUY
        order_req.price = '31.05'
        order_req.quantity = 700

        order_req = dtp_api.OrderReq()
        batch_order_req.append(order_req)
        order_req.account_no = self.account_no
        order_req.code = '002230'
        order_req.order_original_id = '993'
        order_req.exchange = dtp_type.EXCHANGE_SZ_A
        order_req.order_type = dtp_type.ORDER_TYPE_LIMIT
        order_req.order_side = dtp_type.ORDER_SIDE_BUY
        order_req.price = '32'
        order_req.quantity = 700

        self.dtp.place_batch_order(batch_order_req, self.account_no)



if __name__ == '__main__':
    pass
    unittest.main()
