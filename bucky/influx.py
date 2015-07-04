import logging

import influxdb

import bucky.client as client
import bucky.names as names


log = logging.getLogger(__name__)


class InfluxDBClient(client.Client):
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
        print influxdb.InfluxDBClient
        self.client = influxdb.InfluxDBClient(**influxdb_params)
        self.database = cfg.influxdb_database
        for database in self.client.get_list_database():
            if database['name'] == self.database:
                return
        self.client.create_database(self.database)

    def send(self, host, name, value, time):
        stat = names.statname(host, name)
        self.client.write_points(
            [
                {
                    'measurement': stat,
                    'tags': {
                    },
                    'fields': {
                        'value': value,
                    },
                    'time': time,
                }
            ],
            database=self.database,
        )


class InfluxDBBatchClient(InfluxDBClient):
    def __init__(self, cfg, pipe):
        super(InfluxDBBatchClient, self).__init__(cfg, pipe)
        self.buffer_size = cfg.influxdb_buffer_size
        self.buffer = []

    def send(self, host, name, value, time):
        stat = names.statname(host, name)
        self.buffer.append((stat, time, value))
        if len(self.buffer) >= self.buffer_size:
            self.client.write_points(
                [{
                    'measurement': stat,
                    'tags': {
                    },
                    'fields': {
                        'value': value,
                    },
                    'time': time,
                } for stat, time, value in self.buffer],
                database=self.database,
            )
            self.buffer = []
