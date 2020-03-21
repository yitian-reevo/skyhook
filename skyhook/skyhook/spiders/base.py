import logging
import scrapy
from pymongo import MongoClient
import redis
from skyhook.plugins.measurement import InfluxDBSDK


class BaseSpider(scrapy.Spider):
    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)

        # db 相关
        self.mongo_client = None
        self.db = None
        self.redis = None
        self.measurement = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=scrapy.signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        try:
            self.mongo_client = MongoClient(self.settings['MONGODB_CON_STR'], socketTimeoutMS=20000, waitQueueTimeoutMS=20000, readPreference='secondaryPreferred')
            self.db = self.mongo_client[self.settings['MONGODB_DB_NAME']]
            logging.info('spider_opened connect mongodb success.')
        except Exception as e:
            logging.error('spider_opened connect mongodb failed, msg[{}]'.format(e))

        if self.settings['REDIS_ENABLED']:
            try:
                self.redis = redis.Redis(host=self.settings['REDIS_HOST'], port=self.settings['REDIS_PORT'], db=self.settings['REDIS_DB'], password=self.settings['REDIS_PASSWORD'])
                if self.redis.ping():
                    self.redis_key = self.settings['REDIS_KEY']
                    logging.info('spider_opened connect redis success.')
                else:
                    raise
            except Exception as e:
                logging.error('spider_opened connect redis failed, msg[{}]'.format(e))

        self.measurement = InfluxDBSDK(spider=self)

    def spider_closed(self, spider):
        """
        处理一些需要在spider closed执行的简单事件，对于较复杂的逻辑，比如说监控scrapy_stat请使用extensions来实现
        """
        if self.measurement.point_list:
            self.measurement.send_points()

        if self.mongo_client is not None:
            self.mongo_client.close()
            logging.info('sipder_closed disconnect mongodb success.')
