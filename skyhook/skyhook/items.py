# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import logging


class CommonItem(scrapy.Item):
    ruleId = scrapy.Field()
    category = scrapy.Field()  # 爬虫的逻辑分类
    title = scrapy.Field()
    author = scrapy.Field()
    url = scrapy.Field()
    timeStamp = scrapy.Field()  # TODO: 定义timestamp的格式？

    bodyText = scrapy.Field()
    embeddedUrls = scrapy.Field()  # body_text中如果包含link，放到这里

    spider = scrapy.Field()
    meta = scrapy.Field()
    gfwBlocked = scrapy.Field()

    latest = scrapy.Field()
    update_latest = scrapy.Field()
    new_latest = scrapy.Field()

    extras = scrapy.Field()  # 其他字段都填写到extras中

    def get_field(self, field):
        try:
            return self[field] if field in self.keys() else self['extras'][field]
        except Exception:
            logging.error('can not get field[{}] in item: {}'.format(field, self))
            raise

    def set_field(self, field, value):
        try:
            if field in self.keys():
                self[field] = value
            else:
                if 'extras' in self.keys() and self['extras']:
                    self['extras'][field] = value
                else:
                    self['extras'] = {field: value}
        except Exception:
            logging.error('can not get field[{}] in item: {}'.format(field, self))
            raise
