# -*- coding: utf-8 -*-
"""
对文档进行标记化处理并创建索引
"""
import re
import uuid
import doctest

STOP_WORDS = set(
    """
    these are from in
    """.split()
)

WORDS_RE = re.compile("[a-z']{2,}")

IDX_PREFIX = 'idx:'

def tokenize(content):
    """
    把输入的内容标记化
    >>> tokenize("These python demo code are from Redis In Action")
    set(['python', 'demo', 'code', 'redis', 'action'])
    """
    words = set()
    for match in WORDS_RE.finditer(content.lower()):
        word = match.group().strip("'")
        if len(word) >= 2:
            words.add(word)
    return words - STOP_WORDS

def index_document(conn, docid, content):
    """
    把文档内容和ID建立反向索引
    """
    words = tokenize(content)

    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd(IDX_PREFIX + word, docid)

    return len(pipeline.execute())

def _set_common(conn, method, names, ttl=30, execute=True):
    """
    进行redis集合计算的公共函数
    """
    sid = str(uuid.uuid4())
    pipeline = conn.pipeline(True) if execute else conn
    names = [IDX_PREFIX + name for name in names]
    getattr(pipeline, method)('idx' + sid, *names)
    pipeline.expire(IDX_PREFIX + sid, ttl)
    if execute:
        pipeline.execute()
    return sid

def intersect(conn, items, ttl=30, _execute=True):
    """
    执行交集计算的辅助函数
    """
    return _set_common(conn, 'sinterstore', items, ttl, _execute)

def union(conn, items, ttl=30, _execute=True):
    """
    执行并集计算的辅助函数
    """
    return _set_common(conn, 'sunionstore', items, ttl, _execute)

def difference(conn, items, ttl=30, _execute=True):
    """
    执行差集计算的辅助函数
    """
    return _set_common(conn, 'sdiffstore', items, ttl, _execute)

QUERY_RE = re.compile("[+-]?[a-z']{2,}")

def parse(query):
    """
    搜索查询语句的语法分析函数
    >>> parse('''
    ... connect +connection +disconnect +disconnection
    ... chat
    ... -proxy -proxies
    ... ''')
    ([['disconnection', 'connection', 'disconnect', 'connect'], ['chat']], ['proxies', 'proxy'])
    """
    exclude_words = set()
    intersect_words = list()
    synonym_words = set()

    for match in QUERY_RE.finditer(query.lower()):
        word = match.group()
        prefix = word[:1]
        if prefix in '+-':
            word = word[1:]
        else:
            prefix = None

        word = word.strip("'")
        if len(word) < 2 or word in STOP_WORDS:
            continue

        if prefix == '-':
            exclude_words.add(word)
            continue

        if synonym_words and not prefix:
            intersect_words.append(list(synonym_words))
            synonym_words = set()
        synonym_words.add(word)

    if synonym_words:
        intersect_words.append(list(synonym_words))

    return intersect_words, list(exclude_words)


if __name__ == '__main__':
    doctest.testmod(verbose=True)


