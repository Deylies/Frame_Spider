from common.config import REDIS_Config
import redis
import functools


class RedisClient(object):
    def __init__(self, key):
        self.key = key
        self.index = 0
        self._db = None

    def close(self):
        self._db.connection_pool.disconnect()

    def connect(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kw):
            self._db = redis.Redis(**REDIS_Config)
            res = func(self, *args, **kw)
            self.close()
            return res
        return wrapper

    @connect
    def init(self):
        """
        初始化
        :return:
        """
        self._db.delete(self.key)

    @connect
    def add(self, value):
        """
        右侧增加，状态码默认0
        :param value:
        :return:
        """
        self._db.rpush(self.key, value)
        self._db.save()

    @connect
    def get(self):
        """
        左侧取数操作，必须先执行check方法，取完将状态码+1
        :return:
        """
        return self._db.lindex(self.key, self.index)

    @connect
    def pop(self):
        """
        左侧删除
        :return:
        """
        self._db.lpop(self.key)

    @connect
    def check(self):
        """
        用于检查队列中是否还存在可用资源
        :return:
        """
        if self._db.lindex(self.key, self.index):
            return True
        else:
            return False


if __name__ == '__main__':
    conn = RedisClient('info_history')
    # conn = RedisClient('url')
    # print(conn.get())
    # print(conn.check())
    print(conn.get())
