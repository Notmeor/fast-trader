import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.skeleton as dtp

import mock_data as my_data


def login():
    dtp_sync_channel = dtp.DtpSyncChannel()
    dtp_sync_channel.connect()

    header = dtp_struct.RequestHeader()
    header.request_id = my_data.generate_request_id()
    body = dtp_struct.LoginAccountRequest()
    body.account_no = "100900501"
    body.password = "xxxx&xxxx"
    payload = dtp.Payload(header, body)

    response_payload = dtp_sync_channel.login_account(payload)
    print("ResponseHeader:")
    print(response_payload.header)
    print("ResponseHeader:")
    print(response_payload.body)

    dtp_sync_channel.disconnect()

if __name__ == '__main__':
    login()
