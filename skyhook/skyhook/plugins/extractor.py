import logging
import re
from lxml import etree, html
import json
from jsonpath_rw import parse
from six import string_types


class Extractor(object):
    def extract(self, text, path, path_type):
        data = []
        if path == '' or path_type == '':
            return text

        if path_type.lower() == 'json':
            data = self.extract_json(text, path)
        if path_type.lower() == 're':
            data = self.extract_re(text, path)
        if path_type.lower() == 'xpath':
            # 去掉不能解析的xml特殊字符
            # text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', text)
            try:
                parser = etree.XMLParser(encoding='utf-8')
                sel = etree.XML(text, parser=parser)
            except Exception:
                parser = html.HTMLParser(encoding='utf-8')
                sel = etree.HTML(text, parser=parser)
            data = self.extract_sel(sel, path, path_type)

        return data

    def extract_json(self, text, path):
        try:
            if not isinstance(text, string_types):
                logging.warning("[extract_json] unknow text type %s" % type(text))
                return []
            data_json = json.loads(text)
            logging.debug("[extract_json] Parse json text successfully.")
            jsonpath_expr = parse(path)
            extract = [match.value for match in jsonpath_expr.find(data_json)]

            # 转换为string list
            for idx, e in enumerate(extract):
                if not isinstance(e, string_types):
                    extract[idx] = json.dumps(e)
            return extract
        except Exception:
            logging.exception("extract_json error: %s" % path)
            raise
    
    def extract_re(self, text, regex):
        """根据re提取元素, 返回string类型list"""
        try:
            if not isinstance(text, string_types):
                logging.warning('[extract_re] unknow text type %s' % type(text))
                return []
            data_re = re.findall(re.compile(r'%s' % regex), text)
            logging.debug('[extract_re] Parse re text successfully.')
            return data_re
        except Exception:
            logging.exception('extract_re error')
            raise

    def extract_sel(self, selector, path, path_type):
        try:
            if not isinstance(selector, etree._Element):
                logging.warning('extract_sel unknown selector type {}'.format(type(selector)))
                return []
            selected_list = selector.xpath(path)
            if not isinstance(selected_list, list):
                selected_list = [selected_list]
            if selected_list:
                if isinstance(selected_list[0], etree._Element):
                    selected_list = [etree.tostring(e) for e in selected_list]
                    selected_list = [e.strip() for e in selected_list]
                else:
                    selected_list = [e.strip() for e in selected_list]
                return selected_list

        except Exception:
            logging.exception('extract_sel failed.')
