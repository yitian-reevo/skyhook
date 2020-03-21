import logging
import re
from skyhook.plugins.extractor import Extractor
from skyhook.items import CommonItem
from skyhook.plugins.processor.general import General


class CommonSpiderStepExecutor(object):
    def __init__(self, response, parsed_list, policy_depth, rule, spider, item=None):
        self.response = response
        self.parsed_list = parsed_list
        self.policy_depth = policy_depth
        self.rule = rule
        self.spider = spider
        self.cur_policy = rule['meta']['policies'][self.policy_depth]
        self.phase = self.cur_policy['phase']
        self.item = item

    def execute(self):
        if self.phase == 'node':
            return self.parse_node()
        elif self.phase == 'field':
            return self.parse_field()
        else:
            logging.error('unknown phase [{}]'.format(self.phase))
            return SkipResult('skip because unknown phase {}'.format(self.phase))

    def parse_node(self):
        data = self.extract_path(extract_info=self.cur_policy, item=self.item)

        if len(data) > 1:
            limit = self.cur_policy.get('limit', [0, None])
            start, end = self.get_list_range(limit=limit)
            data = data[start:end]

        return NodeStepResult(data=data)

    def parse_field(self):
        if not self.item:  # step=1
            logging.info('node phase: totally get {} nodes.'.format(len(self.parsed_list)))
            items = []
            for parsed_text in self.parsed_list:
                item = CommonItem()
                item['spider'] = self.rule['spider']
                item['category'] = self.rule['category']
                item['meta'] = self.rule['meta']
                item['gfwBlocked'] = self.rule.get('gfwBlocked', False)
                item['ruleId'] = str(self.rule['_id'])
                fill_result = self.fill_fields(item=item, parsed_text=parsed_text)
                if not isinstance(fill_result, SkipResult):
                    items.append(item)
                else:
                    logging.info(fill_result.msg)
            return NodePhaseResult(items)
        else:
            fill_result = self.fill_fields(item=self.item)
            if not isinstance(fill_result, SkipResult):
                return FieldStepResult(skip=False, item=self.item)
            else:
                logging.debug(fill_result.msg)
                return FieldStepResult(skip=True)

    def fill_fields(self, item, parsed_text=None):
        try:
            if 'fields' in self.cur_policy and self.cur_policy['fields']:
                fields = self.cur_policy['fields']
                for idx, field in enumerate(fields):
                    if field.get('type', '') == 'template':
                        # 先不处理template类型的字段
                        continue

                    logging.debug(field)
                    data = self.extract_path(extract_info=field, item=item, parsed_text=parsed_text)
                    cur_field = field['tag']
                    logging.debug(data)

                    # TODO: 这里应该是可以优化的
                    if field['tag'] in self.spider.settings['COMMON_SPIDER_FIELDS_TAG']['multi_value']:
                        item[cur_field] = [e for e in data]
                    elif field['tag'] in self.spider.settings['COMMON_SPIDER_FIELDS_TAG']['single_value']:
                        if field['tag'] == 'url':
                            if data:
                                item['url'] = data[0].decode('utf-8') if not isinstance(data[0], str) else data[0]
                            else:
                                item['url'] = self.rule['meta']['startUrl']
                        else:
                            item[cur_field] = ''.join([e.decode('utf-8') if not isinstance(e, str) else e for e in data])
                    else:
                        item.set_field(cur_field, ''.join([e.decode('utf-8') if not isinstance(e, str) else e for e in data]))

                # 处理template类型的
                template_fields = []
                for field in fields:
                    if field.get('type', '') == 'template':
                        template_fields.append(field)

                for field in template_fields:
                    cur_field = field['tag']
                    template_str = field['path']
                    template_field_name = re.findall(re.compile(r"\$\{(.*?)\}"), template_str)

                    for field_name in template_field_name:
                        template_str = template_str.replace("${%s}" % field_name, item.get_field(field_name))

                    aft_fn = field.get("aft_fn", "")
                    if aft_fn:
                        func = getattr(General, aft_fn[0], None)
                        logging.debug('execute function: {}'.format(aft_fn))
                        this = {'spider': self.spider, 'rule': self.rule, 'item': item, 'data': data}
                        if len(aft_fn) > 1 and aft_fn[1]:
                            data = func(this, **aft_fn[1])
                        else:
                            data = func(this)
                        template_str = "".join([e for e in data])

                    # logging.info('fill template field: {}, content: {}'.format(field, template_str))
                    try:
                        item[cur_field] = template_str
                    except Exception:
                        if "extras" in item.keys():
                            item["extras"][cur_field] = template_str
                        else:
                            item["extras"] = {cur_field: template_str}
            return
        except Exception:
            logging.exception('fill_fields failed.')
            return SkipResult("skip because fill_fields failed.")

    def extract_path(self, extract_info, item, parsed_text=None):
        parsed_list = [parsed_text] if parsed_text else self.parsed_list
        path = extract_info.get('path', '')
        path_type = extract_info.get('type', '')
        aft_fn = extract_info.get('aft_fn', '')
        data = []

        try:
            extractor = Extractor()
            data = extractor.extract(parsed_list[0], path, path_type)
            if not path:
                logging.info('can not get extracted data because path is null, so use the current parsed_text as data.')
                data = parsed_list

            if aft_fn:
                func = getattr(General, aft_fn[0], None)
                logging.debug('execute function: {}'.format(aft_fn))
                this = {'spider': self.spider, 'rule': self.rule, 'item': item, 'data': data}
                if len(aft_fn) > 1 and aft_fn[1]:
                    data = func(this, **aft_fn[1])
                else:
                    data = func(this)

            return data
        except Exception:
            logging.exception('extract_path error, rule: {}'.format(self.rule))
            return []

    def get_list_range(self, limit=[0, None]):
        if len(limit) <= 1:
            start = int(limit[1]) if len(limit) == 1 else 0
            end = None
        else:
            start = int(limit[0])
            end = int(limit[1]) if limit[1] is not None else None
        return start, end


class NodePhaseResult(object):
    def __init__(self, items):
        self.nodes = []
        for idx, item in enumerate(items):
            self.nodes.append({'item': item})


class FieldStepResult(object):
    def __init__(self, skip=False, item=None):
        self.skip = skip
        self.item = item


class NodeStepResult(object):
    def __init__(self, data=None, skip=False):
        self.data = data


class SkipResult(object):
    def __init__(self, msg):
        self.msg = msg
