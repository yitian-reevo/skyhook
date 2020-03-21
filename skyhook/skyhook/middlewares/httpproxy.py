import logging

from scrapy.exceptions import IgnoreRequest
from skyhook.plugins.proxy import ProxyPool


class HttpProxyMiddleware(object):
    """
    HTTP代理中间件
    检查每个request与response的代理设置，如果meta['proxy_policies'], 则进行代理设置以及根据response反馈代理的健康状况。
    """

    def __init__(self):
        self.proxy_pool = None

    def process_request(self, request, spider):
        if not request.meta.get('proxy_policies'):
            return None

        # 如果response中记录了上一次proxy，则重复利用
        # 要利用此特性需要在yield Request时将response放入新request的meta中
        if request.meta.get('response') and request.meta['response'].meta.get('proxy'):
            logging.debug('Detect existed proxy in meta["response"], use: {}'.format(request.meta['response'].meta.get('proxy')))
            request.meta['proxy'] = request.meta['response'].meta.get('proxy')
            return None

        policy = request.meta['proxy_policies']

        if self.proxy_pool is None:
            self.proxy_pool = ProxyPool(spider)

        src_proxy = self.proxy_pool.get_proxy(policy)

        if not src_proxy:
            logging.exception('cannot get proxy, pls double check your proxy pool.')
            return None

        proxy = self._format_proxy(src_proxy)

        request.meta['_src_proxy'] = src_proxy  # 保存未处理格式的第三方代理, 供请求失败时参考

        if proxy:
            request.meta['proxy'] = proxy
            # Pragma is the HTTP/1.0 implementation and cache-control is the HTTP/1.1 implementation of the same concept.
            # They both are meant to prevent the client from caching the response.
            # Older clients may not support HTTP/1.1 which is why that header is still in use.
            request.headers['Pragma'] = 'no-cache'
            request.headers['Cache-Control'] = 'no-cache'
            request.meta['dont_cache'] = True
            logging.info('get proxy: {}'.format(proxy))
        else:
            logging.warning('get proxy failed.')

    def process_response(self, request, response, spider):
        """
        对代理请求的结果进行反馈,
        此方法适合请求成功，但响应内容错误或包含验证码等情景，需在这个方法中进行判断。
        TODO
        """
        return response

    def process_exception(self, request, exception, spider):
        """
        对代理请求的结果进行反馈,
        对于请求失败的页面，调用代理池的异常处理方法
        """
        if request.meta.get('proxy_policies'):
            logging.debug(exception)
            self.proxy_pool.handle_proxy_exception(request.meta.get('_src_proxy'), request.meta.get('proxy_policies'))
        return None

    def _format_proxy(self, proxy_dict):
        return '{}://{}:{}'.format('https' if 'https' in proxy_dict['http_type'].lower() else 'http', proxy_dict['ip'], proxy_dict['port'])
