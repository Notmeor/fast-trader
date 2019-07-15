# -*- coding: utf-8 -*-

import os
import zmq


class Manager:

    def __init__(self):
        self._contexts = {}

    @property
    def context(self):
        _id = os.getpid()
        if _id not in self._contexts:
            self._contexts[_id] = zmq.Context()
        return self._contexts[_id]


manager = Manager()
