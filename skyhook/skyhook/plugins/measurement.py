import logging
import time
from contextlib import contextmanager
from datetime import datetime

import influxdb


class Measurement(object):
    def __init__(self, measurement, tags, fields):
        self.measurement = measurement
        self.tags = tags
        self.fields = fields
        self.time = datetime.utcnow().isoformat() + 'Z'


class InfluxDBSDK(object):
    DB_TYPE = ['mongo', 'mysql', 'redis']
    DB_OPERATION = ['read', 'insert', 'update', 'delete']

    def __init__(self, spider=None):
        self.point_list = []
        self.spider = spider
        self.settings = self.spider.settings
        try:
            if self.settings['INFLUXDB_ENABLED']:
                self.client = influxdb.InfluxDBClient(self.settings['INFLUXDB_HOST'], self.settings['INFLUXDB_PORT'],
                                                      self.settings['INFLUXDB_USER'], self.settings['INFLUXDB_PASSWORD'],
                                                      self.settings['INFLUXDB_DATABASE'], timeout=5)
                self.client.create_database(self.settings['INFLUXDB_DATABASE'])
                logging.info('InfluxDBSDK connect infludb success.')
        except Exception as e:
            logging.exception('InfluxDBSDK connect infludb failed. Msg: {}'.format(e))

    def add_point(self, measurement, tags, fields):
        self.point_list.append(Measurement(measurement, tags, fields).__dict__)

    def send_points(self):
        try:
            if self.point_list and self.settings['INFLUXDB_ENABLED']:
                self.client.write_points(self.point_list)
                logging.info('write points success, cnt[{}]'.format(len(self.point_list)))
                logging.debug('write points: {}'.format(self.point_list))
        except Exception as e:
            logging.exception('send points failed. Msg: {}', format(e))
        finally:
            self.point_list = []
