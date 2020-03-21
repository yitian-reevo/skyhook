from skyhook.items import CommonItem
from datetime import datetime


class commDBPipeline(object):
    """
    通用item管道，将item写入db
    """

    def process_item(self, item, spider):
        if isinstance(item, CommonItem):
            data = {
                'ruleId': item.get('ruleId', ''),
                'category': item.get('category', ''),
                'url': item.get('url', ''),
                'title': item.get('title', ''),
                'author': item.get('author', ''),
                'timeStamp': item.get('timeStamp', ''),
                'bodyText': item.get('bodyText', ''),
                'embeddedUrls': item.get('embeddedUrls', []),
            }

            if 'extras' in item.keys() and item['extras']:
                for k, v in item['extras'].items():
                    data[k] = v

            data['updatedAt'] = datetime.utcnow()
            try:
                spider.db.message.insert_one(data)
            except Exception:
                pass
        return item
