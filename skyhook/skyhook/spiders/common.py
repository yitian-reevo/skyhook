import json
import logging

import scrapy
from bson.objectid import ObjectId

from skyhook.plugins.common_spider_step_executor import (CommonSpiderStepExecutor,
                                                         FieldStepResult,
                                                         NodePhaseResult,
                                                         NodeStepResult,
                                                         SkipResult)
from skyhook.spiders.base import BaseSpider


class CommonSpider(BaseSpider):
    name = 'common'
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 10
    }

    def __init__(self, rules=[], **kwargs):
        self.rules = rules

    def __get_request(self, url, callback, meta, rule_meta, response=None, priority=0):
        cookies = {}
        headers = {
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }
        return scrapy.Request(url, method=rule_meta.get('method', 'GET'), body=rule_meta.get('body', ''), encoding='utf-8', cookies=cookies, callback=callback, dont_filter=True, meta=meta, headers=headers, priority=priority)

    def join_template(self, rule):
        if not rule.get('template'):
            return rule

        # TODO: 做一层template的cache

        template = self.db.template.find_one({'_id': ObjectId(str(rule['template']))})
        if not template:
            logging.warning('cannot find template for rule: {}'.format(rule['_id']))
            return None

        logging.info('using template: {}'.format(template['_id']))
        logging.debug('template content: {}'.format(template))

        # begin join template
        slots = template['slots']
        defaults = template.get('default', {})
        meta_str = json.dumps(template['meta'])
        for idx, slot in enumerate(slots):
            if idx < len(rule['meta']['parameters']):
                val = rule['meta']['parameters'][idx]
            else:
                val = slot.get('default')
            if val is None:
                val = 'null'
            elif isinstance(val, bool):
                val = 'true' if val else 'false'
            elif isinstance(val, int) or isinstance(val, float):
                val = str(val)
            meta_str = meta_str.replace('[[{}]]'.format(idx), val)

        template_meta = json.loads(meta_str)
        if template_meta and isinstance(template_meta, dict):
            for k, v in template_meta.items():
                rule['meta'][k] = v

            # 填充一些默认值
            for k, v in defaults.items():
                if not rule.get(k):
                    rule[k] = v
            return rule

        return None

    def start_requests(self):
        logging.info('totally {} rules will be requested.'.format(len(self.rules)))
        for rule in self.rules:
            try:
                rule = self.join_template(rule)
                if not rule:
                    logging.warning('join template failed, skip...')
                    continue
                logging.debug('join template result: {}'.format(rule))

                meta = rule['meta']

                if len(meta.get('policies', [])) < 1:  # 空policy，直接退出
                    logging.warning('null policies, skip...')
                    continue

                policy_depth, start_url = 0, meta['startUrl']
                if not start_url:
                    logging.warning('startUrl is null, skip...')
                    continue

                yield self.__get_request(start_url, callback=self.parse_node_phase, meta={
                    'policy_depth': policy_depth,  # 接下来要处理的policy的depth
                    'rule': rule,
                    'handle_httpstatus_list': [401],
                    'dont_redirect': False
                }, rule_meta=meta)  # 这里的request对象的meta参数，不是rule中的meta
            except Exception as e:
                logging.exception("start_requests error, msg: {}".format(str(e)))

    def parse_node_phase(self, response):
        rule = response.meta['rule']
        depth = response.meta['policy_depth']
        rule_meta = rule['meta']

        parsed_list = [response.text]
        for policy_depth, policy in enumerate(rule_meta['policies']):
            if policy_depth < depth:
                continue

            logging.info('Starting the policy: {}/{}'.format(policy_depth + 1, len(rule_meta['policies'])))
            parser = CommonSpiderStepExecutor(response=response, parsed_list=parsed_list, policy_depth=policy_depth, rule=rule, spider=self)
            parse_result = parser.execute()
            if isinstance(parse_result, NodeStepResult):
                if len(parse_result.data) == 0:
                    logging.info('get 0 item from node phase, exit...')
                    return
                parsed_list = parse_result.data
                continue
            elif isinstance(parse_result, NodePhaseResult):
                for idx, node in enumerate(parse_result.nodes):
                    yield node['item']
                return
            elif isinstance(parse_result, SkipResult):
                logging.error(parse_result.msg)
                return
            else:
                logging.error('parse_node_phase failed, unknown parser_result type: {}'.format(type(parse_result)))
