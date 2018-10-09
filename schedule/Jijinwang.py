from common.until import printf, logging_except, logging_sql
from db.DB_Redis import RedisClient
from project.Jijinwang import Spider_basic_list as MS, Spider_List


class Schedule(object):
    def __init__(self):
        self.main = self.main_spider
        self.spider_list = Spider_List

    def main_spider(self):
        Main = MS()
        Main.init()
        Main.start()
        Main.close()

    def start(self):
        logging_except(module='init')
        logging_sql()
        main_init = MS()
        main_init.init()
        main_init.close()
        while True:
            main_redis = RedisClient("info_basic_info")
            if not main_redis.get():
                self.main_spider()
            main_redis.close()
            for cls in self.spider_list:
                spider_name = cls.__name__
                redis_key = spider_name.replace("Spider", "info")
                redis_client = RedisClient(redis_key)
                if redis_client.get():
                    self.work(cls)
                redis_client.close()
            printf("Wait for all spider end!")

    def work(self, spider_cla):
        spider = spider_cla()
        try:
            spider.start()
        except Exception as e:
            logging_except(e, cls_name=spider_cla.__name__+':'+str(spider.url))
            print(e)
        finally:
            spider.pop()
            spider.close()


if __name__ == "__main__":
    logging_except("????",cls_name="dsadsadsa")
    # program = Schedule()
    # program.start()
