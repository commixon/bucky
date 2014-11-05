# -*- coding: utf-8 -
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# Copyright 2011 Cloudant, Inc.

import six
import sys
import time
import socket
import struct
import logging
import threading
import collections
try:
    import cPickle as pickle
except ImportError:
    import pickle

import bucky.client as client
import bucky.names as names


if six.PY3:
    xrange = range


log = logging.getLogger(__name__)


class DebugSocket(object):
    def sendall(self, data):
        sys.stdout.write(data)


class BaseCarbonManager(object):
    def __init__(self, cfg, **kwargs):
        self.debug = cfg.debug
        self.ip = kwargs.get('ip', cfg.graphite_ip)
        self.port = kwargs.get('port', cfg.graphite_port)
        self.max_reconnects = kwargs.get('max_reconnects',
                                         cfg.graphite_max_reconnects)
        self.reconnect_delay = kwargs.get('reconnect_delay',
                                          cfg.graphite_reconnect_delay)
        self.backoff_factor = kwargs.get('backoff_factor',
                                         cfg.graphite_backoff_factor)
        self.backoff_max = kwargs.get('backoff_max', cfg.graphite_backoff_max)
        if self.max_reconnects <= 0:
            self.max_reconnects = sys.maxint
        self.connect()

    def connect(self):
        if self.debug:
            log.debug("Connected the debug socket.")
            self.sock = DebugSocket()
            return
        reconnect_delay = self.reconnect_delay
        for i in xrange(self.max_reconnects):
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.sock.connect((self.ip, self.port))
                log.info("Connected to Carbon at %s:%s", self.ip, self.port)
                return
            except socket.error as e:
                if i >= self.max_reconnects:
                    raise
                log.error("Failed to connect to %s:%s: %s", self.ip, self.port, e)
                if reconnect_delay > 0:
                    time.sleep(reconnect_delay)
                    if self.backoff_factor:
                        reconnect_delay *= self.backoff_factor
                        if self.backoff_max:
                            reconnect_delay = min(reconnect_delay, self.backoff_max)
        raise socket.error("Failed to connect to %s:%s after %s attempts", self.ip, self.port, self.max_reconnects)

    def reconnect(self):
        self.close()
        self.connect()

    def close(self):
        try:
            self.sock.close()
        except:
            pass

    def send(self, host, name, value, mtime):
        raise NotImplementedError()


class PlaintextCarbonManager(BaseCarbonManager):
    def send(self, host, name, value, mtime):
        stat = names.statname(host, name)
        mesg = "%s %s %s\n" % (stat, value, mtime)
        for i in xrange(self.max_reconnects):
            try:
                self.sock.sendall(mesg)
                return
            except socket.error as err:
                log.error("Failed to send data to Carbon server: %s", err)
                try:
                    self.reconnect()
                except socket.error as err:
                    log.error("Failed reconnect to Carbon server: %s", err)
        log.error("Dropping message %s", mesg)


class PickleCarbonManager(BaseCarbonManager):
    def __init__(self, cfg, **kwargs):
        super(PickleCarbonManager, self).__init__(cfg, **kwargs)
        self.buffer_size = kwargs.pop('pickle_buffer_size',
                                      cfg.graphite_pickle_buffer_size)
        self.buffer = []

    def send(self, host, name, value, mtime):
        stat = names.statname(host, name)
        self.buffer.append((stat, (mtime, value)))
        if len(self.buffer) >= self.buffer_size:
            self.transmit()

    def transmit(self):
        payload = pickle.dumps(self.buffer, protocol=-1)
        header = struct.pack("!L", len(payload))
        self.buffer = []
        for i in xrange(self.max_reconnects):
            try:
                self.sock.sendall(header + payload)
                return
            except socket.error as err:
                log.error("Failed to send data to Carbon server: %s", err)
                try:
                    self.reconnect()
                except socket.error as err:
                    log.error("Failed reconnect to Carbon server: %s", err)
        log.error("Dropping buffer!")


class CarbonClient(client.Client):
    def __init__(self, cfg, pipe):
        super(CarbonClient, self).__init__(pipe)
        self.managers = []
        if cfg.graphite_hosts:
            defaults = {
                'ip': cfg.graphite_ip,
                'port': cfg.graphite_port,
                'max_reconnects': cfg.graphite_max_reconnects,
                'reconnect_delay': cfg.graphite_reconnect_delay,
                'backoff_factor': cfg.graphite_backoff_factor,
                'backoff_max': cfg.graphite_backoff_max,
                'pickle_enabled': cfg.graphite_pickle_enabled,
                'pickle_buffer_size': cfg.graphite_pickle_buffer_size,
            }
            for item in cfg.graphite_hosts:
                kwargs = defaults.copy()
                kwargs.update(item)
                if kwargs.pop('pickle_enabled'):
                    manager = PickleCarbonManager
                else:
                    del kwargs['pickle_buffer_size']
                    manager = PlaintextCarbonManager
                self.managers.append(manager(cfg, **kwargs))
        elif cfg.graphite_pickle_enabled:
            self.managers.append(PickleCarbonManager(cfg))
        else:
            self.managers.append(PlaintextCarbonManager(cfg))

    def send(self, host, name, val, mtime):
        for manager in self.managers:
            manager.send(host, name, val, mtime)


class ThreadedCarbonClient(CarbonClient):
    def __init__(self, cfg, pipe):
        super(ThreadedCarbonClient, self).__init__(cfg, pipe)
        self.deques = [collections.deque(maxlen=20000)
                       for i in range(len(self.managers))]

    def run(self):
        threads = []
        for manager, deque in zip(self.managers, self.deques):
            threads.append(threading.Thread(target=self.run_thread,
                                            args=(manager, deque)))
            threads[-1].start()

        super(ThreadedCarbonClient, self).run()

    def run_thread(self, manager, deque):
        while True:
            try:
                sample = deque.popleft()
            except IndexError:
                time.sleep(0.1)
                continue
            if sample is None:
                return
            manager.send(*sample)

    def send(self, host, name, val, mtime):
        for deque in self.deques:
            deque.append((host, name, val, mtime))


def get_carbon_client(cfg):
    if cfg.graphite_threads:
        return ThreadedCarbonClient
    return CarbonClient
