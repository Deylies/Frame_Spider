class MysqlClient_ValueError(Exception):
    def __init__(self):
        Exception.__init__(self)

    def __str__(self):
        return repr('未知数据表')


class MysqlClient_StorageError(Exception):
    def __init__(self):
        Exception.__init__(self)

    def __str__(self):
        return repr('输入数据和表结构不符')

class MysqlClient_LengthError(Exception):
    def __init__(self):
        Exception.__init__(self)

    def __str__(self):
        return repr('输入数据长度错误')


class SpiderStructError(Exception):
    def __init__(self):
        Exception.__init__(self)

    def __str__(self):
        return repr('网页结构不符合爬虫')
