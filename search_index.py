# -*- coding: utf-8 -*-
"""
对文档进行标记化处理并创建索引
"""
import re
import uuid

STOP_WORDS = set(
    """
    able about if in into is it its just
    """.split()
)

WORDS_RE = re.compile("[a-z']{2,}")

def tokenize(content):
    """
    把输入的内容标记化
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
        pipeline.sadd('idx:' + word, docid)

    return len(pipeline.execute())

def _set_common(conn, method, names, ttl=30, execute=True):
    """
    进行redis集合计算的公共函数
    """
    sid = str(uuid.uuid4())
    pipeline = conn.pipeline(True) if execute else conn
    names = ['idx:' + name for name in names]
    getattr(pipeline, method)('idx' + sid, *names)
    pipeline.expire('idx:' + sid, ttl)
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


