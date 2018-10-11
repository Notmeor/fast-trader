# -*- coding: utf-8 -*-

import zmq
import random
import time

port = '5556'
pub_server_name = 'pub-server01'

ctx = zmq.Context()
socket = ctx.socket(zmq.SUB)
# socket.connect('tcp://localhost:{}'.format(port))
socket.connect('tcp://192.168.211.194:7779')


socket.subscribe('')

# topicfilter = "10001"  
# socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)  

data = []
while True:
    res = socket.recv()
    print(res)
    data.append(res)
    time.sleep(1)