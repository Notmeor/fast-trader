# -*- coding: utf-8 -*-

import os
# os.environ['FAST_TRADER_CONFIG'] = \
#     '/Users/eisenheim/Documents/git/fast-trader/tmp/config.yaml'

import time
import threading
import unittest

from fast_trader.dtp_trade import dtp_api, DTP_, Dispatcher, dtp_type


class TestDTP(unittest.TestCase):

    def setUp(self):
        self.dtp = DTP_(Dispatcher())
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

#    def test_counter_report(self):
#        
#        self.counter_report_thread = threading.Thread(
#            target=self.dtp.process_counter_report)
#
#        self.counter_report_thread.start()


if __name__ == '__main__':
    pass
    print(f'pid={os.getpid()}, tid={threading.get_ident()}')
    dtp = DTP_(Dispatcher())
    
    dtp.start()
    dtp.process_counter_report()
        
#     def recv():
#         dtp = DTP_(Dispatcher())
#         dtp.start()
#         dtp.process_counter_report()
    
#     counter_report_thread = threading.Thread(
#         target=dtp.process_counter_report)
    
# #    import multiprocessing
# #    counter_report_thread = multiprocessing.Process(
# #        target=recv)

#     print('daemon: ', counter_report_thread.daemon)
#     counter_report_thread.start()
#     print('thread started...')
#     print(counter_report_thread)
    
#     while True:
#         time.sleep(1)
#         print('------not blocked!!-----')


    # counter_report_thread.join()

#
#    unittest.main()
