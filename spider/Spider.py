from selenium import webdriver
from pyvirtualdisplay import Display
import time
from common.setting import FireFoxBinary
from bs4 import BeautifulSoup
import random
from HTTP.HTTP_proxy import proxy
from db.DB_Mysql import Mysql_Client
from db.DB_Redis import RedisClient
from common.until import printf
from common.config import MYSQL_conf

class Webdriver(object):
    def __init__(self):
        self._display = Display(visible=0, size=(800, 800))
        self._display.start()
        self._brower = webdriver.Firefox(firefox_binary=FireFoxBinary, proxy=proxy, timeout=30)

    def get(self, url):
        time.sleep(1 + random.random())
        self._brower.get(url)
        time.sleep(3)
        return BeautifulSoup(self._brower.page_source, 'lxml')

    def find_element_by_xpath(self, xpath):
        return self._brower.find_element_by_xpath(xpath)

    def close(self):
        self._brower.close()
        self._display.stop()


class Spider(object):
    def __init__(self):
        self.profect = None
        self._webdriver = None
        self._redisclient = None
        self._redisclient_code = None
        self.url = None
        self.code = None
        self._mysqlclient = None

    def initialization(self, RedisName, MysqlName,project=None):
        self.profect = project
        self._webdriver = Webdriver()
        self._redisclient = RedisClient(RedisName)
        self._redisclient_code = RedisClient(RedisName + "_code")
        self.url = self._redisclient.get()
        self.code = self._redisclient_code.get()
        self._mysqlclient = Mysql_Client(MysqlName,MYSQL_conf(project))
        printf("Spider start:%s!" % self.__class__.__name__, self.url)

    def close(self):
        self._webdriver.close()
        self._mysqlclient.close()
        self._redisclient.close()

    def storage(self, values, sel):
        self._mysqlclient.storage(values, sel)

    def source(self):
        return BeautifulSoup(self._webdriver._brower.page_source, 'lxml')

    def pop(self):
        self._redisclient.pop()
        self._redisclient_code.pop()
