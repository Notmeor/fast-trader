import unittest

import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.skeleton as dtp

import mock_data as my_data
import mock_dtp_req_channel as dtp_mocker

mocker = dtp_mocker.DtpServerSyncChannelMocker()

class TestLoginAccount(unittest.TestCase):

    def setUpClass():
        mocker.start()

    def tearDownClass():
        mocker.terminate()

    def setUp(self):
        '''call before every test case.'''
        self.dtp_sync_channel = dtp.DtpSyncChannel()
        self.dtp_sync_channel.connect()

    def tearDown(self):
        '''call after every test case.'''
        self.dtp_sync_channel.disconnect()

    def test_should_login_successful_given_right_account(self):
        # given
        login_header = dtp_struct.RequestHeader()
        login_header.request_id = my_data.generate_request_id()
        login_body = dtp_struct.LoginAccountRequest()
        login_body.account_no = my_data.account_no_a
        login_body.password = my_data.account_no_pwd_dict[my_data.account_no_a]
        login_payload = dtp.Payload(login_header, login_body)

        # when
        response_payload = self.dtp_sync_channel.login_account(login_payload)

        # then
        self.assertEqual(response_payload.header.request_id, login_header.request_id)
        self.assertEqual(response_payload.header.code, dtp_type.RESPONSE_CODE_OK)
        self.assertEqual(response_payload.body.token, my_data.account_no_token_dict[login_body.account_no])

    def test_should_login_failed_given_wrong_pwd(self):
        # given
        login_header = dtp_struct.RequestHeader()
        login_header.request_id = my_data.generate_request_id()
        login_body = dtp_struct.LoginAccountRequest()
        login_body.account_no = my_data.account_no_a
        login_body.password = my_data.pwd_unkown
        login_payload = dtp.Payload(login_header, login_body)

        # when
        response_payload = self.dtp_sync_channel.login_account(login_payload)

        # then
        self.assertEqual(response_payload.header.request_id, login_header.request_id)
        self.assertEqual(response_payload.header.code, dtp_type.RESPONSE_CODE_UNAUTHORIZED)

    def test_should_login_failed_given_unknown_account(self):
        # given
        login_header = dtp_struct.RequestHeader()
        login_header.request_id = my_data.generate_request_id()
        login_body = dtp_struct.LoginAccountRequest()
        login_body.account_no = my_data.account_no_unknown
        login_body.password = my_data.account_no_pwd_dict[my_data.account_no_a]
        login_payload = dtp.Payload(login_header, login_body)

        # when
        response_payload = self.dtp_sync_channel.login_account(login_payload)

        # then
        self.assertEqual(response_payload.header.request_id, login_header.request_id)
        self.assertEqual(response_payload.header.code, dtp_type.RESPONSE_CODE_UNAUTHORIZED)

    def test_should_login_failed_given_empty_account(self):
        # given
        login_header = dtp_struct.RequestHeader()
        login_header.request_id = my_data.generate_request_id()
        login_body = dtp_struct.LoginAccountRequest()
        login_body.account_no = my_data.account_no_empty
        login_body.password = my_data.account_no_unknown
        login_payload = dtp.Payload(login_header, login_body)

        # when
        response_payload = self.dtp_sync_channel.login_account(login_payload)

        # then
        self.assertEqual(response_payload.header.request_id, login_header.request_id)
        self.assertEqual(response_payload.header.code, dtp_type.RESPONSE_CODE_BAD_REQUEST)

    def test_should_logout_given_normal_token(self):
        # given
        logout_header = dtp_struct.RequestHeader()
        logout_header.request_id = my_data.generate_request_id()
        logout_body = dtp_struct.LogoutAccountRequest()
        logout_body.account_no = my_data.account_no_a
        logout_payload = dtp.Payload(logout_header, logout_body)

        # when
        response_payload = self.dtp_sync_channel.logout_account(logout_payload)

        # then
        self.assertEqual(response_payload.header.request_id, logout_header.request_id)
        self.assertEqual(response_payload.header.code, dtp_type.RESPONSE_CODE_OK)

    def test_should_query_capital_successful_given_right_account(self):
        # given
        header = dtp_struct.RequestHeader()
        header.request_id = my_data.generate_request_id()
        body = dtp_struct.QueryCapitalRequest()
        body.account_no = my_data.account_no_a
        query_payload = dtp.Payload(header, body)

        # when
        response_payload = self.dtp_sync_channel.query_capital(query_payload)

        # then
        self.assertEqual(response_payload.header.request_id, header.request_id)
        self.assertEqual(response_payload.header.code, dtp_type.RESPONSE_CODE_OK)
        self.assertEqual(response_payload.body.account_no, my_data.account_no_a)

if __name__ == '__main__':
    unittest.main()