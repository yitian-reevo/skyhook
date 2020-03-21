# from functools import reduce


def beautify(content, semis=['\r', '\n', '\r\n']):
    if not content:
        return content

    ret = content
    for semi in semis:
        ret = ret.replace(semi, '')
    # reduce(lambda x, y: x.replace(y, ''), semi, content)
    return ret


def pretend_https(url):
    return 'https:' + url if url.startswith('//') else url
