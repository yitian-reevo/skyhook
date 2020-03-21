# -*- coding: utf-8 -*-

from croniter import croniter
from datetime import datetime


class CronIter(object):
    @classmethod
    def is_valid(cls, str):
        """校验一个cron字符串是不是有效的"""
        try:
            return croniter.is_valid(str)
        except Exception:
            return False

    @classmethod
    def cron_interval_unit(cls, cron_attrs):
        for e in cron_attrs:
            if not e.startswith('*'):
                return False
        return True

    @classmethod
    def balance_cron(cls, balance_key, cron_str):
        if not isinstance(balance_key, str) or not cls.is_valid(cron_str):
            return cron_str

        cron_attrs = cron_str.strip().split(' ')
        if (cron_attrs[0].startswith('*/') or cron_attrs[0] == '0') and cls.cron_interval_unit(cron_attrs[1:]):
            interval = 60 if cron_attrs[0] == '0' else int(cron_attrs[0].replace('*/', ''))
            if 0 <= interval <= 60:
                start_point = int(balance_key, 16) % interval
                balanced_attr0 = []
                for i in range(int(60 / interval)):
                    balanced_attr0.append(str(start_point + i * interval))
                cron_attrs[0] = ','.join(balanced_attr0)

        new_cron_str = ' '.join(cron_attrs)

        return new_cron_str if cls.is_valid(new_cron_str) else cron_str

    @classmethod
    def get_next_cron_time(cls, cron_str, base):
        if not cls.is_valid(cron_str):
            return None
        # TODO: 做成不同类型的返回值, datetime, week, day, etc.
        return croniter(cron_str, base).get_next(datetime)
