import zmq
import time
import threading

import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.dtp_api_id as dtp_api_id

import mock_data as my_data
import mock_dtp_pub_channel as dtp_pub_channel

class DtpServerAsyncChannelMocker:
    def __init__(self):
        self.context = zmq.Context()
        self.socket_dealer = self.context.socket(zmq.DEALER)
        self.socket_dealer.bind("tcp://127.0.0.1:9101")

    def start(self):
        self._running = True
        threading.Thread(target=self._start_asyc_channel).start()

    def terminate(self):
        self._running = False

    def _start_asyc_channel(self):
        while self._running:
            try:
                # Wait for next request from client
                frame1 = self.socket_dealer.recv(flags=zmq.NOBLOCK)
                frame2 = self.socket_dealer.recv()
            except zmq.ZMQError as e:
                time.sleep(0.000001)
                continue

            request_header = dtp_struct.RequestHeader()
            request_header.ParseFromString(frame1)
            place_order_body = dtp_struct.PlaceOrder()
            place_order_body.ParseFromString(frame2)

            if(request_header.api_id == dtp_api_id.PLACE_BATCH_ORDER):
                # fake
                continue

            if(place_order_body.order_original_id == my_data.unnormal_order_01.original_id):
                print(".............mocking report: compliance failed")
                report_header = dtp_struct.ReportHeader()
                report_header.api_id = dtp_api_id.PLACE_REPORT
                report_header.code = dtp_type.RESPONSE_CODE_FORBIDDEN
                report_header.message = "compliance failed."
                report_body = dtp_struct.PlacedReport()
                report_body.order_original_id = my_data.unnormal_order_01.original_id
                report_payload = my_data.Payload(report_header, report_body)

                topic = place_order_body.account_no
                dtp_pub_channel.publish_compliacne_failed_report(topic, report_payload)
            elif(request_header.token == my_data.account_no_token_dict[place_order_body.account_no]):
                print(".............mocking report: fill")
                report_header = dtp_struct.ReportHeader()
                report_header.api_id = dtp_api_id.PLACE_REPORT
                report_header.code = dtp_type.RESPONSE_CODE_OK
                report_header.message = "success."
                report_body = dtp_struct.PlacedReport()
                report_body.order_exchange_id = my_data.normal_order_01_placing.exchange_id
                report_body.status = my_data.normal_order_01_placing.status
                report_payload = my_data.Payload(report_header, report_body)

                topic = place_order_body.account_no
                dtp_pub_channel.publish_order_report(topic, report_payload)

                report_header = dtp_struct.ReportHeader()
                report_header.api_id = dtp_api_id.FILL_REPORT
                report_header.code = dtp_type.RESPONSE_CODE_OK
                report_header.message = "success."
                report_body = dtp_struct.FillReport()
                report_body.fill_exchange_id = my_data.normal_order_01_fill.fill_exchange_id
                report_body.order_exchange_id = my_data.normal_order_01_placing.exchange_id
                report_body.fill_status = my_data.normal_order_01_fill.fill_status
                report_payload = my_data.Payload(report_header, report_body)

                topic = place_order_body.account_no
                dtp_pub_channel.publish_order_report(topic, report_payload)
            else:
                print(".............mocking report: unknown token")
                report_header = dtp_struct.ReportHeader()
                report_header.api_id = dtp_api_id.PLACE_REPORT
                report_header.code = dtp_type.RESPONSE_CODE_UNAUTHORIZED
                report_header.message = "unknown token."
                report_body = dtp_struct.PlacedReport()
                report_payload = my_data.Payload(report_header, report_body)

                topic = place_order_body.account_no
                dtp_pub_channel.publish_order_report(topic, report_payload)

if __name__ == '__main__':
    pass