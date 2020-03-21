import time

from scrapy.crawler import CrawlerRunner
from scrapy.utils import project
from scrapy.utils.log import configure_logging, logger
from twisted.internet import defer, reactor

from skyhook.schedulers.cronjob import CronJobScheduler
from skyhook.spiders.common import CommonSpider


settings = project.get_project_settings()
configure_logging(settings)
runner = CrawlerRunner(settings)

sche = CronJobScheduler()


@defer.inlineCallbacks
def crawl():
    while True:
        try:
            rules, sleep_time = sche.tick()
            yield runner.crawl(CommonSpider, rules=rules)
            logger.info('sleep in {} seconds...'.format(sleep_time))
            time.sleep(sleep_time)
        except Exception:
            time.sleep(3)

    reactor.stop()


if __name__ == '__main__':
    crawl()
    reactor.run()
    # while True:
    #     rules, sleep = sche.tick()
    #     logger.info('rules: {}, sleep: {}'.format(rules, sleep))
    #     time.sleep(sleep)
