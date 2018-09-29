### directory
    /dtp            DTP接口、脚手架
        mock_*      mock数据
        test_*      使用样例，以单元测试为例
        sample.py   使用样例

### environment

    \>= Python 3.6.1 64-bit

    \>= protobuf 3.5.2

    \>= pyzmq 17.0.0

### unit test command

    python -m unittest

### sample command

    python mock_dtp_req_channel.py

    python sample.py



### 试一下TDD驱动dtp skeleton开发

##### spike: 先找个unit test例子

    test_spike.py
    cmd: python -m unittest test_spike

##### spike: 用UT试一下dtp proto的定义

    test_dtp_spike.py
    cmd: python -m unittest test_dtp_spike

##### spike: 先搞一下zmq怎么调用，试一下req模式下，多个客户端连同一个服务端

    zmq_spike_client_account_a.py
    zmq_spike_client_account_b.py
    zmq_spike_server.py

##### account api: let first test case fail

    test_login_account.py


##### account api: let first test case pass

    skeleton.py
    mock dtp server: mock_dtp_req_channel.py

    cmd: python mock_dtp_req_channel.py
    cmd: python -m unittest test_login_account

##### account api: second case: wrong pwd

##### account api: third case: unknown account

##### order api: first case: normal order

    test_place_order.py

    cmd: python mock_dtp_req_channel.py
    cmd: python mock_dtp_dealer_channel.py
    cmd: python -m unittest test_place_order

##### order api: send case: unknown token
