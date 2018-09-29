#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Hello World server in Python
# Binds REP socket to tcp://*:5555
# Expects "Hello" from client, replies with "World"
#
import zmq
import dtp.type_pb2 as dtp_type
import dtp.api_pb2 as dtp_struct
import dtp.dtp_api_id as dtp_api_id

import mock_data as my_data

context = zmq.Context()
socket_pub_order_report = context.socket(zmq.PUB)
socket_pub_order_report.bind("tcp://127.0.0.1:9002")
# socket_pub.setsockopt(zmq.TCP_ACCEPT_FILTER, b'127.1.1.1')
socket_pub_compliance_failed_report = context.socket(zmq.PUB)
socket_pub_compliance_failed_report.bind("tcp://127.0.0.1:9102")

def publish_order_report(topic, payload):
    print("publish_order_report publishing...")
    socket_pub_order_report.send_string(topic, zmq.SNDMORE)
    socket_pub_order_report.send(payload.header.SerializeToString(), zmq.SNDMORE)
    socket_pub_order_report.send(payload.body.SerializeToString())
    print("publish_order_report published")

def publish_compliacne_failed_report(topic, payload):
    print("publish_compliacne_failed_report publishing...")
    socket_pub_compliance_failed_report.send_string(topic, zmq.SNDMORE)
    socket_pub_compliance_failed_report.send(payload.header.SerializeToString(), zmq.SNDMORE)
    socket_pub_compliance_failed_report.send(payload.body.SerializeToString())
    print("publish_compliacne_failed_report published")