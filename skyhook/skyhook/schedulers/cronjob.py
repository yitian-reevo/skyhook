"""
定制的定时任务调度器

standlone version
"""

from collections import namedtuple
from datetime import datetime, timedelta

from bson.objectid import ObjectId
from pymongo import MongoClient
from scrapy.utils import project
from scrapy.utils.log import configure_logging, logger

from skyhook.utils.cron import CronIter
from skyhook.utils.heap import HeapManager

# 堆中的数据格式
heap_rule_t = namedtuple('heap_rule_t', ['next_call_time', 'id', 'cron_updatedAt'])


class CronJobScheduler(object):
    def __init__(self, *args, **kwargs):
        # 读取scrapy project的配置项
        self.settings = project.get_project_settings()
        configure_logging(self.settings)

        # db
        self.mongo_client = MongoClient(self.settings['MONGODB_CON_STR'], socketTimeoutMS=20000, waitQueueTimeoutMS=20000, readPreference='secondaryPreferred')
        self.db = self.mongo_client[self.settings['MONGODB_DB_NAME']]

        self.schedule = {}
        self._heap = HeapManager()
        self.need_rule_fields = {
            '_id': 1,
            'cron': 1,
            'spider': 1,
            'status': 1,
            'gfwBlocked': 1,
            'updatedAt': 1,
            'crawlerName': 1,
            'meta': 1,
            'latest': 1,
            'template': 1,
            'createdAt': 1,
            'category': 1
        }
        self.cron_balanced_spiders = ['common']
        self.need_init_sche = True
        self.last_time_pull_rules_from_db = datetime.utcnow()
        self.pull_sche_interval = 60

    def str_dict_object(self, item):
        """
        将item dict中value类型为object的对象转化为string类型
        """
        for k, v in item.items():
            if isinstance(v, ObjectId):
                item[k] = str(v)
            if isinstance(v, datetime):
                item[k] = v.strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(v, dict):
                self.str_dict_object(v)

    def sync_sche_to_heap(self, changed_rules):
        rules_to_heap_cnt = 0
        rules_delete_from_sche = 0

        logger.info('begin to process changed rules.')
        for rules_processed_cnt, rule in enumerate(changed_rules, 1):
            if rule['status'].lower() == 'run':
                old_rule = self.schedule.get(rule['_id'])
                if not old_rule or old_rule['cron'] != rule['cron']:  # 新增了rule或者旧的rule的cron字段更新了
                    rule['cron_updatedAt'] = datetime.utcnow()
                    next_call_time = CronIter.get_next_cron_time(rule.get('balance_cron') or rule.get('cron'), datetime.now())
                    if not next_call_time:
                        continue

                    rule_t = heap_rule_t(next_call_time, rule['_id'], rule['cron_updatedAt'])
                    self._heap.push(rule_t)
                    rules_to_heap_cnt += 1
                    self.schedule[rule['_id']] = rule
                else:
                    # TODO: FIX THIS BUG
                    # self.schedule[rule['_id']].update(rule)
                    # 这里如果py<3.6有个bug: https://bugs.python.org/issue6766
                    # 考虑手动更新schedule, 目前会产生变化的只有cron_updatedAt字段
                    rule['cron_updatedAt'] = old_rule['cron_updatedAt']
                    self.schedule[rule['_id']] = rule
            else:
                if rule['_id'] in self.schedule:
                    del self.schedule[rule['_id']]
                    rules_delete_from_sche += 1

            if rules_processed_cnt % 1e4 == 0:
                logger.info('1W rules have been processed. {} left.'.format(len(changed_rules) - rules_processed_cnt))
        logger.info('finish to process changed rules.')
        logger.info('{} rules have been pushed to heap in this interval.'.format(rules_to_heap_cnt))
        logger.info('{} rules have been removed from schedule in this interval.'.format(rules_delete_from_sche))
        logger.info('Currently {} rules in the schedule.'.format(len(self.schedule)))

    def update_sche(self):
        logger.info('begin updating schedule.')
        cursor = None

        if self.need_init_sche:
            logger.info('initializing schedule at the first time.')
            current_time = datetime.utcnow()
            cursor = self.db.rule.find({
                'updatedAt': {
                    '$lte': current_time
                },
                'status': 'RUN',
            }, projection=self.need_rule_fields)
            self.need_init_sche = False
            self.last_time_pull_rules_from_db = current_time
        else:
            easy_time = self.last_time_pull_rules_from_db - timedelta(minutes=2)
            cursor = self.db.rule.find({
                'updatedAt': {
                    '$gt': easy_time
                },
            }, projection=self.need_rule_fields)
            self.last_time_pull_rules_from_db = datetime.utcnow()

        logger.info('begin to init or update rules.')
        changed_rules = []
        balanced_rules_cnt = 0

        for rule in cursor:
            self.str_dict_object(rule)
            if rule['spider'] in self.cron_balanced_spiders:
                rule['balance_cron'] = CronIter.balance_cron(rule['_id'], rule['cron'])
                balanced_rules_cnt += 1
            changed_rules.append(rule)

        logger.info('{} rules have been updated during this interval.'.format(len(changed_rules)))
        logger.debug('{} rules have been balanced during this interval.'.format(balanced_rules_cnt))

        if changed_rules:
            self.sync_sche_to_heap(changed_rules)
        logger.info('finish updating schedule.')

    def run(self):
        """执行一次拉取操作，同时返回下一次拉取需要的等待时间"""
        current_time = datetime.utcnow()
        time_delta = current_time - self.last_time_pull_rules_from_db

        if self.need_init_sche or time_delta.seconds >= self.pull_sche_interval:
            self.update_sche()

    def __when(self, next_call_time):
        now = datetime.now()
        # 如果直接减, ().seconds会返回一个很大的值
        return -1 if next_call_time < now else (next_call_time - now).seconds

    def __populate(self, rule):
        """将列表中的数据计算下次执行时间后推回堆中"""
        next_call_time = CronIter.get_next_cron_time(rule.get('balance_cron') or rule.get('cron'), datetime.now())
        self._heap.push(heap_rule_t(next_call_time, rule['_id'], rule['cron_updatedAt']))

    def tick(self):
        rules_to_be_returned = []
        delayed_rules_cnt = 0
        while True:
            try:
                self.run()

                entry = self._heap.pop()
                logger.info('length: {}'.format(self._heap.length()))
                if not entry:
                    logger.info('no rules in heap now.')
                    return rules_to_be_returned, self.pull_sche_interval

                if not isinstance(entry, heap_rule_t):
                    logger.Warning('detect an entry does not fit heap_rule_t struct: {}'.format(entry))
                    continue

                rule = self.schedule.get(entry.id)
                if not rule:
                    logger.info('detect a rule that has been removed from sche, discard, ruieId[{}].'.format(entry.id))
                    continue

                if entry.cron_updatedAt == rule['cron_updatedAt']:
                    delay = self.__when(entry.next_call_time)

                    if delay < 0:
                        rules_to_be_returned.append(rule)
                        self.__populate(rule)
                        delayed_rules_cnt += 1
                    else:
                        self._heap.push(entry)
                        logger.info('{} rules were delayed since last tick.'.format(delayed_rules_cnt))
                        return rules_to_be_returned, delay
                else:
                    # 过期了，目前只能是因为cron字段被更新过了，这条rule作废，因为最新的rule已经被推送进了堆
                    logger.info('detect an outdated rule, discard, ruleId[{}].'.format(entry.id))
            except Exception as e:
                logger.exception('tick catches an exception. Msg: {}'.format(e))
                return [], self.pull_sche_interval


if __name__ == '__main__':
    rules, sleep = CronJobScheduler().tick()
    logger.info('rules: {}, sleep: {}'.format(rules, sleep))
