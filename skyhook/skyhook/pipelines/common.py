from bson.objectid import ObjectId

from skyhook.items import CommonItem


class commLatestManagerPipeline(object):
    """
    通用item管道，更新rule的latest字段
    """

    def process_item(self, item, spider):
        if isinstance(item, CommonItem):
            if 'update_latest' in item.keys() and item['update_latest']:
                spider.db.rule.update_one({
                    '_id': ObjectId(item['ruleId'])
                }, {
                    '$set': {
                        'latest': item['new_latest']
                    },
                    '$currentDate': {'updatedAt': True}
                })

        return item
