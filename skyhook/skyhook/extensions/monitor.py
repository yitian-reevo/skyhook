from datetime import datetime

from scrapy import signals


class MonitorExtension(object):
    """
    采集并上报监控指标，如scrapy状态或需要的业务指标
    """

    def __init__(self, stats):
        self.stats = stats
        self.spider = None

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.stats)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        self.spider = spider

    def spider_closed(self, spider):
        if not spider.settings['INFLUXDB_ENABLED']:
            return

        # 添加scrapy监控数据
        src_stats = self.stats.get_stats()
        src_stats['duration_seconds'] = (src_stats['finish_time'] - src_stats['start_time']).seconds
        src_stats['start_time'] = datetime.strftime(src_stats['start_time'], '%Y-%m-%dT%X%Z')
        src_stats['finish_time'] = datetime.strftime(src_stats['finish_time'], '%Y-%m-%dT%X%Z')

        des_stats = {}
        for key in src_stats:
            des_stats[key.replace('/', '_').lower()] = src_stats[key]
        des_stats['log_count_error'] = des_stats.get('log_count_error', 0)
        des_stats['downloader_exception_count'] = des_stats.get('downloader_exception_count', 0)
        spider.measurement.add_point('scrapy', {
            'spider': spider.name,
        }, des_stats)

        # 发送监控数据
        spider.measurement.send_points()
