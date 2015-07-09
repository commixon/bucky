import logging

import influxdb

import bucky.client as client
import bucky.names as names


log = logging.getLogger(__name__)


class InfluxDBClient(client.KVNameClient):
    def __init__(self, cfg, pipe):
        super(InfluxDBClient, self).__init__(pipe)
        influxdb_params = {}
        for key in ('ip', 'port', 'username', 'password',
                    'ssl', 'verify_ssl', 'timeout', 'use_udp', 'udp_port'):
            attr = 'influxdb_%s' % key
            if hasattr(cfg, attr):
                influxdb_params[key] = getattr(cfg, attr)
        if 'ip' in influxdb_params:
            influxdb_params['host'] = influxdb_params.pop('ip')
        self.client = influxdb.InfluxDBClient(**influxdb_params)
        self.database = cfg.influxdb_database
        for database in self.client.get_list_database():
            if database['name'] == self.database:
                return
        self.client.create_database(self.database)

    def get_point(self, host, name, value, time):
        tags = {'host': host}
        if isinstance(name, str):
            measurement = name
        else:
            measurement = 'main'
            tags.update(name)
        return {
            'measurement': measurement,
            'tags': tags,
            'fields': {'value': value},
            'time': time,
        }

    def send(self, host, name, value, time):
        self.client.write_points([self.get_point(host, name, value, time)],
                                 database=self.database)


class InfluxDBBatchClient(InfluxDBClient):
    def __init__(self, cfg, pipe):
        super(InfluxDBBatchClient, self).__init__(cfg, pipe)
        self.buffer_size = cfg.influxdb_buffer_size
        self.buffer = []

    def send(self, host, name, value, time):
        self.buffer.append((host, name, value, time))
        if len(self.buffer) >= self.buffer_size:
            self.client.write_points([self.get_point(*sample)
                                      for sample in self.buffer],
                                     database=self.database)
            self.buffer = []
