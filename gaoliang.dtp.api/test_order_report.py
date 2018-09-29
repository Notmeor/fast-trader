import queue
import time
import threading
import unittest

import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.skeleton as dtp

import mock_data as my_data
import mock_dtp_dealer_channel as dtp_mocker

mocker = dtp_mocker.DtpServerAsyncChannelMocker()

class TestComplianceReport(unittest.TestCase):

    def setUpClass():
        mocker.start()

    def tearDownClass():
        mocker.terminate()

    def setUp(self):
        self.lock = threading.Lock()
        '''call before every test case.'''
        self.dtp_async_channel = dtp.DtpAsyncChannel()
        self.dtp_async_channel.connect()
        self.dtp_subscribe_channel = dtp.DtpSubscribeChannel()
        self.dtp_subscribe_channel.connect()
        self.dtp_subscribe_channel.register_compliance_callback(self.compiance_failed_callback)
        self.dtp_subscribe_channel.register_counter_callback(
            self.place_report_callback, self.fill_report_callback, self.cancel_report_callback)
        self.sub_compliance_failed_report_queue = queue.Queue()
        self.sub_place_report_queue = queue.Queue()
        self.sub_fill_report_queue = queue.Queue()
        self.sub_cancel_report_queue = queue.Queue()
        time.sleep(0.1)

    def tearDown(self):
        '''call after every test case.'''
        self.dtp_async_channel.disconnect()
        self.dtp_subscribe_channel.disconnect()

    def test_should_be_place_failed_given_unkown_token(self):
        # given
        order_header = dtp_struct.RequestHeader()
        order_header.request_id = my_data.generate_request_id()
        order_header.token = my_data.token_unknown
        order_body = dtp_struct.PlaceOrder()
        self._init_order_body(order_body, my_data.normal_order_01)
        order_payload = dtp.Payload(order_header, order_body)
        self.dtp_subscribe_channel.start_subscribe_report(topic=order_body.account_no)
        self.starting_async_fetch_report(self.async_fetch_place_report)

        # when
        self.dtp_async_channel.place_order(order_payload)
        print(".............place_order: unknown token")

        # then
        report_payload = self.fetch_place_report()
        self.assertEqual(report_payload.header.code, dtp_type.RESPONSE_CODE_UNAUTHORIZED)

    def test_should_be_placing_given_normal_order(self):
        # given
        order_header = dtp_struct.RequestHeader()
        order_header.request_id = my_data.generate_request_id()
        order_header.token = my_data.account_no_token_dict[my_data.account_no_a]
        order_body = dtp_struct.PlaceOrder()
        self._init_order_body(order_body, my_data.normal_order_01)
        order_payload = dtp.Payload(order_header, order_body)
        self.dtp_subscribe_channel.start_subscribe_report(topic=order_body.account_no)
        self.starting_async_fetch_report(self.async_fetch_place_report)

        # when
        self.dtp_async_channel.place_order(order_payload)
        print(".............place_order: normal order for placing")

        # then
        report_payload = self.fetch_place_report()
        self.assertEqual(report_payload.header.code, dtp_type.RESPONSE_CODE_OK)
        self.assertEqual(report_payload.body.order_exchange_id,
            my_data.normal_order_01_placing.exchange_id)
        self.assertEqual(report_payload.body.status, my_data.normal_order_01_placing.status)

    def test_should_be_filled_given_normal_order(self):
        # given
        order_header = dtp_struct.RequestHeader()
        order_header.request_id = my_data.generate_request_id()
        order_header.token = my_data.account_no_token_dict[my_data.account_no_a]
        order_body = dtp_struct.PlaceOrder()
        self._init_order_body(order_body, my_data.normal_order_01)
        order_payload = dtp.Payload(order_header, order_body)
        self.dtp_subscribe_channel.start_subscribe_report(topic=order_body.account_no)
        self.starting_async_fetch_report(self.async_fetch_fill_report)

        # when
        self.dtp_async_channel.place_order(order_payload)
        print(".............place_order: normal order for fill")

        # then
        report_payload = self.fetch_fill_report()
        self.assertEqual(report_payload.header.code, dtp_type.RESPONSE_CODE_OK)
        self.assertEqual(report_payload.body.fill_exchange_id,
            my_data.normal_order_01_fill.fill_exchange_id)
        self.assertEqual(report_payload.body.order_exchange_id,
            my_data.normal_order_01_placing.exchange_id)
        self.assertEqual(report_payload.body.fill_status, my_data.normal_order_01_fill.fill_status)

    def test_should_be_compliace_failed_given_unnormal_order(self):
        # given
        order_header = dtp_struct.RequestHeader()
        order_header.request_id = my_data.generate_request_id()
        order_header.token = my_data.account_no_token_dict[my_data.account_no_a]
        order_body = dtp_struct.PlaceOrder()
        self._init_order_body(order_body, my_data.unnormal_order_01)
        order_payload = dtp.Payload(order_header, order_body)
        self.dtp_subscribe_channel.start_subscribe_report(topic=order_body.account_no)
        self.starting_async_fetch_report(self.async_fetch_compliance_failed_report)

        # when
        self.dtp_async_channel.place_order(order_payload)
        print(".............place_order: unnormal order for compliance fail")

        # then
        report_payload = self.fetch_compliance_failed_report()
        self.assertEqual(report_payload.header.code, dtp_type.RESPONSE_CODE_FORBIDDEN)
        self.assertIsNotNone(report_payload.header.message)
        self.assertNotEqual(report_payload.header.message, "")
        self.assertEqual(report_payload.body.order_original_id, my_data.unnormal_order_01.original_id)

    @unittest.skip("TBC...")
    def test_fake_place_batch_order(self):
        # given
        batch_header = dtp_struct.RequestHeader()
        batch_body = dtp_struct.PlaceBatchOrder()
        batch_payload = dtp.Payload(batch_header, batch_body)

        # when
        self.dtp_async_channel.place_batch_order(batch_payload)

        # then
        # fake

    def starting_async_fetch_report(self, async_fetch_func):
        threading.Thread(target=async_fetch_func).start()

    def compiance_failed_callback(self, response_payload):
        print(".............compliance_failed_callback start")
        self.sub_compliance_failed_report_queue.put(response_payload)
        print(".............compliance_failed_callback finished")

    def place_report_callback(self, response_payload):
        print(".............place_report_callback start")
        self.sub_place_report_queue.put(response_payload)
        print(".............place_report_callback finished")

    def fill_report_callback(self, response_payload):
        print(".............fill_report_callback start")
        self.sub_fill_report_queue.put(response_payload)
        print(".............fill_report_callback finished")

    def cancel_report_callback(self, response_payload):
        print(".............cancel_report_callback start")
        self.sub_cancel_report_queue.put(response_payload)
        print(".............cancel_report_callback finished")

    def async_fetch_compliance_failed_report(self):
        print(".............async_fetch_compliance_failed_report start")
        self.lock.acquire()
        self.report_payload = self.sub_compliance_failed_report_queue.get()
        self.lock.release()
        print(".............async_fetch_compliance_failed_report finished")

    def async_fetch_place_report(self):
        print(".............async_fetch_place_report start")
        self.lock.acquire()
        self.report_payload = self.sub_place_report_queue.get()
        self.lock.release()
        print(".............async_fetch_place_report finished")

    def async_fetch_fill_report(self):
        print(".............async_fetch_fill_report start")
        self.lock.acquire()
        self.report_payload = self.sub_fill_report_queue.get()
        self.lock.release()
        print(".............async_fetch_fill_report finished")

    def async_fetch_cancel_report(self):
        pass

    def fetch_compliance_failed_report(self):
        print(".............fetch_compliance_failed_report start")
        self.lock.acquire()
        report_payload = self.report_payload
        self.lock.release()
        print(".............fetch_compliance_failed_report finished")
        return report_payload

    def fetch_place_report(self):
        print(".............fetch_place_report start")
        self.lock.acquire()
        report_payload = self.report_payload
        self.lock.release()
        print(".............fetch_place_report finished")
        return report_payload

    def fetch_fill_report(self):
        print(".............fetch_fill_report start")
        self.lock.acquire()
        report_payload = self.report_payload
        self.lock.release()
        print(".............fetch_fill_report finished")
        return report_payload

    def fetch_cancel_report(self):
        pass

    def _init_order_body(self, order, mock_order):
        order.account_no = mock_order.account_no
        order.order_original_id = mock_order.original_id
        order.exchange = mock_order.exchange
        order.code = mock_order.code
        order.price = mock_order.price
        order.quantity = mock_order.quantity
        order.order_side = mock_order.order_side
        order.order_type = mock_order.order_type

if __name__ == '__main__':
    unittest.main()