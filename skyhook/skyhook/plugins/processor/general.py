import logging
import re
import time

from skyhook.plugins.extractor import Extractor


def catch_exception(func):
    def wrapper(*args, **kwargs):
        try:
            rnt = func(*args, **kwargs)
            return rnt
        except Exception:
            logging.exception('execute {}() failed, return [""], item: %s'.format(func.__name__, args))
            return ['']
    return wrapper


def extract(text, path, path_type):
    extractor = Extractor()
    data = extractor.extract(text, path, path_type)
    return data


class General(object):
    @staticmethod
    def extract_items_from_orders_ids(this, unique_path, path_type):
        # TODO
        pass

    @staticmethod
    def extract_items_from_disorder_ids(this, unique_path, path_type):
        data = this['data']
        rule = this['rule']
        latest = rule.get('latest', '')
        slice_idx = len(data)
        new_latest = ''

        for idx, node in enumerate(data):
            unique_id = ''.join(extract(node, unique_path, path_type))
            if not latest:
                new_latest = unique_id
                break
            if not new_latest:
                new_latest = unique_id
            if latest == unique_id:
                slice_idx = idx
                break
        rule['new_latest'] = new_latest
        logging.debug('update new latest: {}'.format(new_latest))
        return data[:slice_idx]

    @staticmethod
    @catch_exception
    def join_str(this, prefix='', suffix=''):
        return ['{}{}{}'.format(prefix, data, suffix) for data in this['data']]

    @staticmethod
    @catch_exception
    def replace_str(this, src='', des=''):
        """
        替换字符串，将src替换为des，其中src为正则表达式
        """
        new_data = []
        for data in this['data']:
            new_data.append(re.sub(re.compile(r'%s'.format(src)), des, data))
        return new_data

    @staticmethod
    @catch_exception
    def get_latest(this):
        item = this['item']
        rule = this['rule']

        if rule.get('new_latest'):
            item['update_latest'] = True
            item['new_latest'] = rule['new_latest']
            rule['new_latest'] = ''
        else:
            item['update_latest'] = False
        return [rule.get('latest', '')]

    @staticmethod
    @catch_exception
    def format_date(this, time_format):
        """
        根据time_format对表示时间的字符串进行格式化，统一返回13位的纯数字格式
        """
        ret = []
        for data in this['data']:
            res = str(int(time.mktime(time.strptime(data, time_format))))
            if len(res) < 13:
                res += '0' * (13-len(res))
            ret.append(res)

        return ret
