import time
from common.setting import  Main_Url, Basic_URL
from bs4 import BeautifulSoup
from db.DB_Mysql import Mysql_Client
from db.DB_Redis import RedisClient
from common.until import printf, remove, get_digit, special_repace,logging_except
from common.errors import SpiderStructError
from spider.Spider import Spider,Webdriver


class Spider_basic_list(Spider):
    """
    Get Jijin name,code,url
    此爬虫生成多个基金url供后续爬虫爬取
    """

    def init(self):
        printf("Main Spider Initization!")
        self._webdriver = Webdriver()
        self._redisclient = RedisClient("info_basic_info")
        self._redisclient_code = RedisClient("info_basic_info" + "_code")
        self._mysqlclient = Mysql_Client("jj_basic_list")
        # self.initialization('Spider_basic_list', 'jj_basic_list')
        # Redis Server initialization
        self._redisclient.init()
        self._redisclient_code.init()
        init_list = [
            'info_manager', 'info_company', 'info_level', 'info_history', 'info_fh', 'info_holds',
            'info_bets_holds', 'info_holds_trend', 'info_trade_pz', 'info_trade_compare',
            'info_property_pz', 'info_changes', 'info_sizechange', 'info_holder_struct', 'info_all_gonggao',
            'info_finance_target', 'info_property_bets', 'info_profit', 'info_income', 'info_cost',
            'info_purchase_info', 'info_swich_info'
        ]
        for client in init_list:
            RedisClient(client).init()
            RedisClient(client + "_code").init()

    def start(self):
        printf("Main Spider Start!")
        self.url = Main_Url
        # get jijin list information
        tags = self._webdriver.get(self.url).find('table', id='oTable').find('tbody').find_all('tr')
        amount = 0
        for tag in tags:
            cols = tag.find_all('td')
            code = cols[2].text
            name = cols[3].find_all('a')[0].text
            url = Basic_URL + cols[3].find_all('a')[-1]['href']
            self.storage([code, name, url], ('code', code))
            self.post(url, code)
            amount += 1
        printf("main process Done,AMOUNT:%s" % amount)

    def post(self, url, code):
        self._redisclient.add(url)
        self._redisclient_code.add(code)


class Spider_basic_info(Spider):
    """
    基本信息爬虫，将生成基本信息url队列
    """

    def get_basic_url(self):
        divided = self.url.split('/')
        divided.remove(divided[-1])
        return "/".join(divided) + '/'

    def start(self):
        self.initialization('info_basic_info', 'jj_info_basic_info')
        self.basic_url = self.get_basic_url()
        source = self._webdriver.get(self.url)
        urls = source.find("div", id="dlcontent").find_all('a')
        self.post(urls)
        # 获取基本信息
        tags = source.find_all('table', class_='info w790')
        tags_1 = tags[0].find_all('tr')
        values = [tags_1[0].find('td').text,
                  get_digit(tags_1[1].find('td').text),
                  tags_1[2].find_all('td')[0].text,
                  tags_1[3].find_all('td')[0].text,
                  tags_1[4].find_all('a')[0].text,
                  tags_1[5].find_all('a')[0].text,
                  tags_1[6].find_all('td')[0].text,
                  tags_1[7].find_all('td')[0].text,
                  tags_1[8].find_all('td')[0].text,
                  tags_1[9].find_all('td')[0].text,
                  tags_1[0].find_all('td')[-1].text,
                  tags_1[1].find_all('td')[-1].text,
                  tags_1[2].find_all('td')[-1].text,
                  tags_1[3].find('a').text,
                  tags_1[4].find_all('a')[-1].text,
                  tags_1[5].find_all('a')[-1].text,
                  tags_1[6].find_all('td')[-1].text,
                  tags_1[7].find_all('td')[-1].text,
                  tags_1[8].find_all('td')[-1].text,
                  tags_1[9].find_all('td')[-1].text]
        # values.append(tags[0].find('td').text)
        # information storage
        self.storage(values, sel=("code", self.code))
        printf("jj_info_basic_info storage Done,CODE:%s" % self.code)

    def post(self, urls):
        client_dict = {
            'info_manager': 2, 'info_level': 4, 'info_history': 6, 'info_fh': 7, 'info_holds': 10,
            'info_bets_holds': 11, 'info_holds_trend': 12, 'info_trade_pz': 13, 'info_trade_compare': 14,
            'info_property_pz': 15, 'info_changes': 16, 'info_sizechange': 17, 'info_holder_struct': 18,
            'info_all_gonggao': 19,
            'info_finance_target': 26, 'info_property_bets': 27, 'info_profit': 28, 'info_income': 29, 'info_cost': 30,
            'info_purchase_info': 31, 'info_swich_info': 32
        }
        for client in client_dict:
            RedisClient(client).add(self.basic_url + urls[client_dict[client]]['href'])
            RedisClient(client + '_code').add(self.code)
        RedisClient('info_company').add(urls[3]['href'])
        RedisClient('info_company' + '_code').add(self.code)


class Spider_history(Spider):
    def start(self):
        self.initialization('info_history', 'jj_history_income')
        self._webdriver.get(self.url)
        # send_box = self._webdriver.find_element_by_xpath("//div[@id='pagebar']//input[1]")

        # botton = self._webdriver.find_element_by_xpath("//div[@id='pagebar']//input[@type='button']")
        next_botton = "//div[@id='pagebar']//label[last()]"
        current_xpath = "//div[@id='pagebar']//label[@class='cur']"
        pages_num = self._webdriver.find_element_by_xpath("//div[@id='pagebar']//label[last()-1]").text
        current_page = self._webdriver.find_element_by_xpath(current_xpath).text
        amount = 0
        while int(current_page) < int(pages_num):
            current_page = self._webdriver.find_element_by_xpath(current_xpath).text
            time.sleep(3)
            tags = BeautifulSoup(self._webdriver._brower.page_source, 'lxml').find('table',
                                                                                   class_="w782 comm lsjz").find(
                'tbody').find_all('tr')
            for tag in tags:
                tars = tag.find_all('td')
                values = []
                values.append(self.code)
                values.append(tars[0].text)
                values.append(tars[1].text)
                values.append(tars[2].text)
                values.append(tars[3].text)
                values.append(tars[4].text)
                values.append(tars[5].text)
                values.append(tars[6].text)
                self._mysqlclient.storage(values, ('code', self.code, 'jz_date', tars[0].text))
                amount += 1
            botton = self._webdriver.find_element_by_xpath(next_botton)
            time.sleep(3)
            botton.click()
            time.sleep(3)
        printf('jj_history_income storage CODE:%s,AMOUNT:%s' % (self.code, amount))


class Spider_manager(Spider):
    def jj_info_manager_changes(self, source):
        box1 = source.find('div', class_='box').find('table').find_all('tr')
        amount = 0
        for tr in box1[1:]:
            value1 = []
            value1.append(self.code)
            value1.append(tr.find_all('td')[0].text)
            value1.append(tr.find_all('td')[1].text)
            value1.append(tr.find_all('td')[2].text)
            value1.append(tr.find_all('td')[3].text)
            value1.append(tr.find_all('td')[4].text)
            sel1 = ('code', self.code, 'start_date', tr.find_all('td')[0].text, 'manager', tr.find_all('td')[2].text)
            self.storage(self._mysqlclient[0], value1, sel1)
            amount += 1
        printf("jj_info_manager_changes storage,CODE:%s,AMOUNT:%s" % (self.code, amount))
        # jj_info_manager_history storage
        box3 = source.find_all('table', class_='w782 comm jloff')[-1].find('tbody').find_all('tr')
        value3s = []
        sel3s = []
        for tr in box3:
            value3 = []
            jjcode = tr.find_all('td')[0].text
            value3.append(jjcode)
            value3.append(tr.find_all('td')[1].text)
            value3.append(tr.find_all('td')[2].text)
            value3.append(tr.find_all('td')[3].text)
            value3.append(tr.find_all('td')[4].text)
            value3.append(tr.find_all('td')[5].text)
            value3.append(tr.find_all('td')[6].text)
            value3.append(tr.find_all('td')[7].text)
            value3.append(tr.find_all('td')[8].text)
            value3s.append(value3)
            sel3s.append(['code', jjcode, 'manager'])
        return value3s, sel3s

    def jj_info_manager(self, source):
        # redict to another page
        box2 = source.find('div', class_='box nb')
        manager_href = box2.find('div', class_='jl_intro').find_all('a')[1]['href']
        source = self._webdriver.get(manager_href)
        # jj_info_manager storage
        manager_source = source.find('div', class_='jlinfo clearfix')
        value2 = []
        value2.append(self.code)
        manager_name = manager_source.find('div', class_='left clearfix w438').find('h3').text.split(':')[-1]
        value2.append(manager_name)

        all_info = manager_source.find('div', class_="right jd ").text.replace(' ', '').split('\n')
        remove('', all_info)
        value2.append(all_info[0].split('：')[1])
        value2.append(all_info[1].split('：')[1])
        value2.append(all_info[2].split('：')[1])
        value2.append(all_info[5])
        value2.append(all_info[-1])
        value2.append(manager_source.find('div', class_='right ms').find('p').text.replace(' ', '').replace('\n', ''))
        sel2 = ("code", self.code, "manager", manager_name)
        self.storage(self._mysqlclient[1], value2, sel2)
        printf("jj_info_manager storage CODE:%s" % self.code)
        return manager_name

    def jj_info_manager_history(self, value3s, sel3s, manager_name):
        amount = 0
        for i in range(len(value3s)):
            value3 = value3s[i]
            value3.insert(0, manager_name)
            sel3 = sel3s[i]
            sel3.append(manager_name)
            self.storage(self._mysqlclient[2], value3, sel3)
            amount += 1
        printf("jj_info_manager_history storage CODE:%s,AMOUNT:%s" % (self.code, amount))

    def start(self):
        self.initialization("info_manager", "jj_info_manager_changes")
        self._mysqlclient = [Mysql_Client("jj_info_manager_changes"), Mysql_Client("jj_info_manager"),
                             Mysql_Client('jj_info_manager_history')]
        # jj_info_manager_changes storage
        source = self._webdriver.get(self.url).find('div', class_='txt_in')
        value3s, sel3s = self.jj_info_manager_changes(source)

        manager_name = self.jj_info_manager(source)

        # jj_info_manager_history storage
        self.jj_info_manager_history(value3s, sel3s, manager_name)

    def close(self):
        self._webdriver.close()
        self._mysqlclient[0].close()
        self._mysqlclient[1].close()
        self._mysqlclient[2].close()

    def storage(self, client, values, sel):
        client.storage(values, sel)


class Spider_company(Spider):
    def new_url(self):
        current_url = self._redisclient.get()
        target_url = \
            self._webdriver.get(current_url).find('div', class_="ttjj-panel").find('a', class_="ttjj-panel-links")[
                'href']
        return Basic_URL[:-1] + target_url

    def jj_info_company(self, source):
        # jj_info_company storage
        box1 = source.find('div', "first-block").find('tbody').find_all('tr')
        company = box1[0].find('td', class_="category-value").text
        value1 = []
        value1.append(self.code)
        value1.append(company)
        value1.append(box1[1].find('td', class_="category-value").text)
        value1.append(box1[2].find('td', class_="category-value").text)
        value1.append(box1[3].find('td', class_="category-value").text)
        value1.append(box1[4].find('td', class_="category-value attached-value fixed-width").text)
        value1.append(box1[5].find('td', class_="category-value").text)
        value1.append(box1[6].find('td', class_="category-value").text)
        value1.append(box1[7].find('td', class_="category-value").text)
        value1.append(box1[8].find('td', class_="category-value").text)
        value1.append(box1[9].find('td', class_="category-value fixed-width").text)
        value1.append(box1[10].find('td', class_="category-value").text)
        value1.append(box1[11].find('td', class_="category-value").text.replace(' ', '').replace('\n', ''))
        value1.append(box1[12].find('td', class_="category-value fixed-width").text.replace(' ', '').replace('\n', ''))
        value1.append(box1[3].find('td', class_="category-value attached-value").text)
        value1.append(box1[4].find('td', class_="category-value attached-value").text)
        value1.append(box1[8].find('td', class_="category-value attached-value").text)
        value1.append(box1[9].find('td', class_="category-value attached-value").text)
        value1.append(box1[12].find('td', class_="category-value attached-value").text)
        sel1 = ('code', self.code, 'name', company)
        self.storage(self._mysqlclient[0], value1, sel1)
        printf("jj_info_company storage CODE:%s" % self.code)
        return company

    def jj_info_company_honor(self, company):
        # jj_info_company_honor storage
        next_botton = "//div[@id='lsryPager']//ul//li//a[@class='next ttjj-iconfont']"
        current_xpath = "//div[@id='lsryPager']//li[@class=' active']"
        pages_num = self._webdriver.find_element_by_xpath("//div[@id='lsryPager']//li[last()-1]//a").text
        current_page = self._webdriver.find_element_by_xpath(current_xpath).text
        amount = 0
        while int(current_page) < int(pages_num):
            current_page = self._webdriver.find_element_by_xpath(current_xpath).text
            time.sleep(3)
            tags = BeautifulSoup(self._webdriver._brower.page_source, 'lxml').find('div', id="lsryContent").find(
                'tbody').find_all('tr')
            for tag in tags:
                tars = tag.find_all('td')
                values = []
                values.append(company)
                values.append(tars[0].text.replace('\n', ''))
                values.append(tars[1].text.replace('\n', ''))
                self.storage(self._mysqlclient[1], values, ('name', company, 'time', tars[0].text))
                amount += 1
            botton = self._webdriver.find_element_by_xpath(next_botton)
            time.sleep(3)
            botton.click()
            time.sleep(3)
        printf("jj_info_company_honor,CODE:%s,AMOUNT:%s" % (self.code, amount))

    def jj_info_company_admin(self, company):
        # jj_info_company_admin storage

        next_botton = "//div[@id='gcglPager']//ul//li//a[@class='next ttjj-iconfont']"
        current_xpath = "//div[@id='gcglPager']//li[@class=' active']"
        pages_num = self._webdriver.find_element_by_xpath("//div[@id='gcglPager']//li[last()-1]//a").text
        current_page = self._webdriver.find_element_by_xpath(current_xpath).text
        amount = 0
        while int(current_page) < int(pages_num):
            current_page = self._webdriver.find_element_by_xpath(current_xpath).text
            time.sleep(3)
            tags = BeautifulSoup(self._webdriver._brower.page_source, 'lxml').find('div', id="gcglBody").find(
                'tbody').find_all('tr')
            for index in range(int(len(tags[1:]) / 2)):
                values = []
                values.append(company)
                info = tags[2 * index + 1].find_all('td')
                name = info[0].text
                values.append(name)
                values.append(info[1].text)
                values.append(info[2].text)
                values.append(info[3].text)
                values.append(tags[2 * index + 2].text.replace(' ', '').replace('\n', ''))
                sel = ('company', company, 'name', name)
                self.storage(self._mysqlclient[2], values, sel)
                amount += 1
            botton = self._webdriver.find_element_by_xpath(next_botton)
            time.sleep(3)
            botton.click()
            time.sleep(3)
        printf("jj_info_company_honor,CODE:%s,AMOUNT:%s" % (self.code, amount))

    def jj_info_company_party(self, company):
        # jj_info_company_party storage
        wyh_botton = \
            self._webdriver._brower.find_elements_by_xpath("//div[@class='third-block']//div[@class='ttjj-tab']//li")[
                -1]
        time.sleep(3)
        wyh_botton.click()
        time.sleep(3)
        next_botton = "//div[@id='wyhPager']//ul//li//a[@class='next ttjj-iconfont']"
        current_xpath = "//div[@id='wyhPager']//li[@class=' active']"
        pages_num = self._webdriver.find_element_by_xpath("//div[@id='wyhPager']//li[last()-1]//a").text
        current_page = self._webdriver.find_element_by_xpath(current_xpath).text
        amount = 0
        while int(current_page) < int(pages_num):
            current_page = self._webdriver.find_element_by_xpath(current_xpath).text
            time.sleep(3)
            tags = BeautifulSoup(self._webdriver._brower.page_source, 'lxml').find('div', id="wyhBody").find(
                'tbody').find_all('tr')
            for index in range(int(len(tags[1:]) / 2)):
                values = []
                values.append(company)
                info = tags[2 * index + 1].find_all('td')
                name = info[0].text
                values.append(name)
                values.append(info[1].text)
                values.append(info[2].text)
                values.append(info[3].text)
                values.append(tags[2 * index + 2].text.replace(' ', '').replace('\n', ''))
                sel = ('company', company, 'name', name)
                self.storage(self._mysqlclient[3], values, sel)
                amount += 1
            botton = self._webdriver.find_element_by_xpath(next_botton)
            time.sleep(3)
            botton.click()
            time.sleep(3)
        printf("jj_info_company_honor,CODE:%s,AMOUNT:%s" % (self.code, amount))

    def start(self):
        self.initialization("info_company", "jj_info_company")
        self.url = self.new_url()
        self._mysqlclient = [Mysql_Client('jj_info_company'), Mysql_Client('jj_info_company_honor'),
                             Mysql_Client('jj_info_company_admin'), Mysql_Client('jj_info_company_party')]
        source = self._webdriver.get(self.url)
        company = self.jj_info_company(source)
        try:
            self.jj_info_company_honor(company)
        except Exception as e:
            logging_except(e)
            printf("jj_info_company_honor", "NoData!!")
        self.jj_info_company_admin(company)
        self.jj_info_company_party(company)
        pass

    def close(self):
        self._webdriver.close()
        for client in self._mysqlclient:
            client.close()

    def storage(self, client, values, sel):
        client.storage(values, sel)


class Spider_level(Spider):
    def start(self):
        self.initialization("info_level", "jj_info_level")
        source = self._webdriver.get(self.url)
        tags = source.find('table', id='fundgradetable').find('tbody').find_all('tr')
        amount = 0
        for tag in tags:
            values = [self.code]
            infos = tag.find_all('td')
            if len(infos) == 5:
                day = infos[0].text
                values.append(day)
                values.append(infos[1].text.count("★"))
                values.append(infos[2].text.count("★"))
                values.append(infos[3].text.count("★"))
                values.append(infos[4].text.count("★"))
                sel = ('code', self.code, 'pj_date', day)
                self.storage(values, sel)
                amount += 1
            else:
                printf("jj_info_level", "Struction Error!")
        printf("jj_info_level storage,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_fh(Spider):
    def start(self):
        self.initialization("info_fh", "jj_info_fh_peisong")
        source = self._webdriver.get(self.url)
        tags = source.find('table', class_='w782 comm cfxq').find('tbody').find_all('tr')
        amount = 0
        for tag in tags:
            values = [self.code]
            infos = tag.find_all('td')
            if len(infos) == 5:
                day = infos[1].text
                values.append(infos[0].text)
                values.append(day)
                values.append(infos[2].text)
                values.append(get_digit(infos[3].text))
                values.append(infos[4].text)
                sel = ('code', self.code, 'quanyidengji_date', day)
                self.storage(values, sel)
                amount += 1
            else:
                printf("jj_info_fh_peisong", "Struction Error!")
        printf("jj_info_fh_peisong storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_holds(Spider):
    def start(self):
        self.initialization("info_holds", "jj_info_holds")
        self._webdriver.get(self.url)
        bot_values = [botton.get_attribute('value') for botton in
                      self._webdriver._brower.find_elements_by_xpath("//div[@id='pagebar']//label")]
        current_xpath = "//div[@id='pagebar']//label[@value='%s']"
        amount = 0
        for bot in bot_values:
            time.sleep(3)
            self._webdriver.find_element_by_xpath(current_xpath % bot).click()
            time.sleep(3)
            source = BeautifulSoup(self._webdriver._brower.page_source, 'lxml')
            dead_line = [i.find('font', class_='px12').text for i in
                         source.find_all('label', class_="right lab2 xq656")]
            # botton.click()
            tables = [i.find('tbody') for i in source.find_all('div', class_="boxitem w790")]
            remove(None, tables)
            for i in range(len(dead_line)):
                season = dead_line[i]
                for tr in tables[i].find_all('tr'):
                    table = tr.find_all('td')

                    values = [self.code, season]
                    gp_code = table[1].text
                    values.append(special_repace(gp_code))
                    values.append(special_repace(table[2].text))
                    if len(table) == 7:
                        values.append(special_repace("$$$".join([i['href'] for i in table[3].find_all('a')])))
                        values.append(special_repace(table[4].text))
                        values.append(special_repace(table[5].text))
                        values.append(special_repace(table[6].text))
                    elif len(table) == 9:
                        values.append(special_repace("$$$".join([i['href'] for i in table[5].find_all('a')[1:]])))
                        values.append(special_repace(table[6].text))
                        values.append(special_repace(table[7].text))
                        values.append(special_repace(table[8].text))
                    else:
                        raise SpiderStructError
                    sel = ('code', self.code, 'seasonal', season, 'gp_code', gp_code)
                    self.storage(values, sel)
                    amount += 1
        printf("jj_info_fh_peisong storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_bets_holds(Spider):
    def start(self):
        self.initialization("info_bets_holds", "jj_info_bets_holds")
        self._webdriver.get(self.url)
        selector = self._webdriver._brower.find_element_by_xpath("//select[@id='zqcc']")
        bottons = self._webdriver._brower.find_elements_by_xpath("//select[@id='zqcc']//option")
        amount = 0
        for botton in bottons:
            selector.click()
            time.sleep(2)
            botton.click()
            time.sleep(2)
            source = BeautifulSoup(self._webdriver._brower.page_source, 'lxml')
            bot_values = [i.find('font', class_='px12').text for i in
                          source.find_all('h4', class_='t')]
            remove(None, bot_values)
            tables = source.find('div', id='cctable').find_all('tbody')
            remove(None, tables)
            for i in range(len(bot_values)):
                seasonal = bot_values[i]
                table = tables[i].find_all('tr')
                amount += len(table)
                for tr in table:
                    values = [self.code, seasonal]
                    bet_code = tr.find_all('td')[1].text
                    for td in tr.find_all('td')[1:]:
                        values.append(td.text)
                    sel = ('code', self.code, 'seasonal', seasonal, 'bet_code', bet_code)
                    self.storage(values, sel)
        printf("jj_info_bets_holds storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_holds_trend(Spider):
    def start(self):
        self.initialization("info_holds_trend", "jj_info_holds_trend")
        self._webdriver.get(self.url)
        next_botton = "//div[@id='pagebar']//label[last()]"
        current_xpath = "//div[@id='pagebar']//label[@class='cur']"
        pages_num = self._webdriver.find_element_by_xpath("//div[@id='pagebar']//label[last()-1]").text
        current_page = self._webdriver.find_element_by_xpath(current_xpath).text
        amount = 0
        while int(current_page) < int(pages_num):
            amount += 1
            current_page = self._webdriver.find_element_by_xpath(current_xpath).text
            time.sleep(3)
            # tags = .find('div', id="wyhBody").find(
            #     'tbody').find_all('tr')
            botton = self._webdriver.find_element_by_xpath(next_botton)
            time.sleep(3)
            botton.click()
            time.sleep(3)
        printf("jj_info_bets_holds storage,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_trade_pz(Spider):
    def start(self):
        self.initialization("info_trade_pz", "jj_info_trade_pz")
        self._webdriver.get(self.url)
        bot_values = [botton.get_attribute('value') for botton in
                      self._webdriver._brower.find_elements_by_xpath("//div[@id='pagebar']//label")]
        current_xpath = "//div[@id='pagebar']//label[@value='%s']"
        amount = 0
        for bot in bot_values:
            time.sleep(3)
            self._webdriver.find_element_by_xpath(current_xpath % bot).click()
            time.sleep(3)
            source = BeautifulSoup(self._webdriver._brower.page_source, 'lxml')
            dead_line = [i.find('font', class_='px12').text for i in
                         source.find_all('label', class_="right lab2 xq656")]
            # botton.click()
            tables = [i.find('tbody') for i in source.find_all('div', class_="boxitem w790")]
            remove(None, tables)
            for i in range(len(dead_line)):
                season = dead_line[i]
                for tr in tables[i].find_all('tr'):
                    amount += 1
                    table = tr.find_all('td')
                    values = [self.code, season]
                    industry_type = table[1].text
                    values.append(industry_type)
                    if len(table) > 4:
                        values.append(table[3].text)
                        values.append(table[4].text)
                    elif len(table) == 4:
                        values.append(table[2].text)
                        values.append(table[3].text)
                    else:
                        raise SpiderStructError
                    sel = ('code', self.code, 'seasonal', season, 'industry_type', industry_type)
                    self.storage(values, sel)
        printf("jj_info_trade_pz storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_trade_compare(Spider):
    def start(self):
        self.initialization("info_trade_compare", "jj_info_trade_compare")
        source = self._webdriver.get(self.url)
        trs = source.find("div", id="hypzsytable").find("tbody").find_all('tr')
        amount = 0
        for tr in trs:
            amount += 1
            values = [self.code]
            tds = tr.find_all('td')
            zjh_code = tds[0].text
            for td in tds:
                values.append(td.text)
            sel = ("code", self.code, "zjh_code", zjh_code)
            self.storage(values, sel)
        printf("jj_info_trade_compare storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_property_pz(Spider):
    def start(self):
        self.initialization("info_property_pz", "jj_info_property_pz")
        ###
        self.url="http://fund.eastmoney.com/f10/zcpz_002423.html"
        ###
        source = self._webdriver.get(self.url)
        trs = source.find("table", class_="w782 comm tzxq").find("tbody").find_all('tr')
        amount = 0
        for tr in trs:
            amount += 1
            values = [self.code]
            tds = tr.find_all('td')
            report_date = tds[0].text
            for td in tds:
                values.append(td.text)
            sel = ("code", self.code, "report_date", report_date)
            self.storage(values, sel)
        printf("jj_info_property_pz storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_changes(Spider):
    def start(self):
        self.initialization("info_changes", "jj_info_changes")
        self._webdriver.get(self.url)
        buy_sale_bottons = [("//ul[@id='zdbdTab']//li[@class='at']", "buy"),
                            ("//ul[@id='zdbdTab']//li[not(@class='at')]", "sale")]
        amount = 0
        for buy_sale_botton in buy_sale_bottons:
            self._webdriver._brower.find_element_by_xpath(buy_sale_botton[0]).click()
            time.sleep(3)
            bot_values = [botton.get_attribute('value') for botton in
                          self._webdriver._brower.find_elements_by_xpath("//div[@id='pagebar']//label")]
            current_xpath = "//div[@id='pagebar']//label[@value='%s']"
            for bot in bot_values:
                time.sleep(3)
                self._webdriver.find_element_by_xpath(current_xpath % bot).click()
                time.sleep(3)
                source = BeautifulSoup(self._webdriver._brower.page_source, 'lxml')
                labels = source.find_all('label', class_="right lab2 xq656")
                if len(labels):
                    dead_line = [i.find('font', class_='px12').text for i in labels]
                    tables = [i.find('tbody') for i in
                              source.find('div', id="bdtable").find_all('div', class_="box")]
                    remove(None, tables)
                    for i in range(len(dead_line)):
                        season = dead_line[i]
                        for tr in tables[i].find_all('tr'):
                            table = tr.find_all('td')
                            values = [self.code, season]
                            gp_code = table[1].text
                            for td in table[1:]:
                                values.append(td.text)
                            values.append(buy_sale_botton[1])
                            sel = (
                                'code', self.code, 'seasonal', season, 'gp_code', gp_code, 'stats', buy_sale_botton[1])
                            self.storage(values, sel)
                            amount += 1
                else:
                    printf("jj_info_trade_pz CODE:%s NO DATA!" % self.code)
        printf("jj_info_trade_pz storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_sizechange(Spider):
    def start(self):
        self.initialization("info_sizechange", "jj_info_sizechange")
        source = self._webdriver.get(self.url)
        trs = source.find('div', id="gmbdtable").find("tbody").find_all('tr')
        amount = 0
        for tr in trs:
            tds = tr.find_all('td')
            if len(tds) == 6:
                amount += 1
                values = [self.code]

                date = tds[0].text
                for td in tds:
                    values.append(td.text)
                sel = ("code", self.code, "date", date)
                self.storage(values, sel)
            else:
                pass
        printf("jj_info_sizechange storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_holder_struct(Spider):
    def start(self):
        self.initialization("info_holder_struct", "jj_info_holder_struct")
        source = self._webdriver.get(self.url)
        trs = source.find('div', id="cyrjgtable").find("tbody").find_all('tr')
        amount = 0
        for tr in trs:
            tds = tr.find_all('td')
            if len(tds) == 5:
                amount += 1
                values = [self.code]
                gonggao_date = tds[0].text
                for td in tds:
                    values.append(td.text)
                sel = ("code", self.code, "gonggao_date", gonggao_date)
                self.storage(values, sel)
            else:
                pass
        printf("jj_info_holder_struct storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_all_gonggao(Spider):
    def start(self):
        self.initialization("info_all_gonggao", "jj_info_all_gonggao")
        self._webdriver.get(self.url)
        js = "window.open('%s');"
        # start
        next_botton = "//div[@id='pagebar']//label[last()]"
        current_xpath = "//div[@id='pagebar']//label[@value='%s']"
        pages_num = self._webdriver.find_element_by_xpath("//div[@id='pagebar']//label[last()-1]").text
        current_page = 0
        amount = 0
        while int(current_page) < int(pages_num):
            current_page += 1
            try:
                self._webdriver.find_element_by_xpath(current_xpath%current_page).click()
            except Exception as e:
                logging_except(e)
                self._webdriver.find_element_by_xpath(next_botton).click()
            time.sleep(3)
            table = BeautifulSoup(self._webdriver._brower.page_source, 'lxml').find('div', id='ggtable').find(
                'tbody').find_all('tr')
            for tr in table:
                tds = tr.find_all('td')
                if len(tds) == 3:
                    amount += 1
                    values = [self.code]
                    title = tds[0].text.replace(' ', '').replace('\n', '')
                    report_type = tds[1].text.replace(' ', '').replace('\n', '')
                    date = tds[-1].text.replace(' ', '').replace('\n', '')
                    href = tds[0].find('a')['href']
                    for td in [title, report_type, date]:
                        values.append(td)
                    self._webdriver._brower.execute_script(js % href)
                    self._webdriver._brower.switch_to_window(self._webdriver._brower.window_handles[1])
                    time.sleep(3)
                    while not BeautifulSoup(self._webdriver._brower.page_source, "lxml").find('pre',
                                                                                              id='jjggzwcontentbody'):
                        time.sleep(3)
                    values.append(
                        special_repace(BeautifulSoup(self._webdriver._brower.page_source, "lxml").find('pre',
                                                                                                       id='jjggzwcontentbody').text))
                    time.sleep(3)
                    self._webdriver._brower.close()
                    self._webdriver._brower.switch_to_window(self._webdriver._brower.window_handles[0])
                    sel = ("code", self.code, "title", title, 'date', date, 'type', report_type)
                    self.storage(values, sel)
                    amount += 1
                else:
                    pass
            # botton = self._webdriver.find_element_by_xpath(next_botton)
            # time.sleep(3)
            # botton.click()
            # time.sleep(3)
        # end
        printf("jj_info_all_gonggao storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_finance_target(Spider):
    def start(self):
        self.initialization("info_finance_target", "jj_info_finance_target")
        self._webdriver.get(self.url)
        bot_values = [botton.get_attribute('value') for botton in
                      self._webdriver._brower.find_elements_by_xpath("//div[@id='pagebar']//label")]
        current_xpath = "//div[@id='pagebar']//label[@value='%s']"
        amount = 0
        for bot in bot_values:
            time.sleep(3)
            self._webdriver.find_element_by_xpath(current_xpath % bot).click()
            time.sleep(3)
            source = BeautifulSoup(self._webdriver._brower.page_source, 'lxml')
            table = source.find('div', id='cwtable').find('div', class_='boxitem w790')
            head = table.find('thead').find_all('th')[1:]
            bodys = table.find_all('tbody')
            for i in range(len(head)):
                values = [self.code]
                amount += 1
                for body in bodys:
                    for tr in body.find_all('tr'):
                        values.append(tr.find_all('td')[1 + i].text)
                values.append(head[i].text)
                sel = ('code', self.code, 'date', values[-1])
                self.storage(values, sel)
        printf("jj_info_finance_target storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_property_bets(Spider):
    def start(self):
        self.initialization("info_property_bets", "jj_info_property_bets")
        self._webdriver.get(self.url)
        bot_values = [botton.get_attribute('value') for botton in
                      self._webdriver._brower.find_elements_by_xpath("//div[@id='pagebar']//label")]
        current_xpath = "//div[@id='pagebar']//label[@value='%s']"
        amount = 0
        for bot in bot_values:
            time.sleep(3)
            self._webdriver.find_element_by_xpath(current_xpath % bot).click()
            time.sleep(3)
            source = BeautifulSoup(self._webdriver._brower.page_source, 'lxml')
            table = source.find('div', id='cwtable').find('div', class_='boxitem w790')
            head = table.find('thead').find_all('th')[1:]
            bodys = table.find_all('tbody')
            for i in range(len(head)):
                values = [self.code]
                amount += 1
                for body in bodys:
                    for tr in body.find_all('tr'):
                        if tr.get('class'):
                            pass
                        else:
                            values.append(tr.find_all('td')[1 + i].text)
                values.append(head[i].text)
                sel = ('code', self.code, 'report_date', values[-1])
                self.storage(values, sel)
        printf("jj_info_property_bets storage ,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_profit(Spider):
    def start(self):
        self.initialization("info_profit", "jj_info_profit")
        self._webdriver.get(self.url)
        bot_values = [botton.get_attribute('value') for botton in
                      self._webdriver._brower.find_elements_by_xpath("//div[@id='pagebar']//label")]
        current_xpath = "//div[@id='pagebar']//label[@value='%s']"
        amount = 0
        for bot in bot_values:
            time.sleep(3)
            self._webdriver.find_element_by_xpath(current_xpath % bot).click()
            time.sleep(3)
            source = BeautifulSoup(self._webdriver._brower.page_source, 'lxml')
            table = source.find('div', id='cwtable').find('div', class_='boxitem w790')
            head = table.find('thead').find_all('th')[1:]
            bodys = table.find_all('tbody')
            for i in range(len(head)):
                values = [self.code, head[i].text]
                amount += 1
                for body in bodys:
                    for tr in body.find_all('tr'):
                        values.append(tr.find_all('td')[1 + i].text)
                # values.append()
                sel = ('code', self.code, 'report_date', head[i].text)
                self.storage(values, sel)
        printf("jj_info_profit storage CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_income(Spider):
    def start(self):
        self.initialization("info_income", "jj_info_income")
        source = self._webdriver.get(self.url)
        table = source.find('div', class_='txt_cont').find('table', class_='w782 comm income').find('tbody').find_all(
            'tr')
        amount = 0
        for tr in table:
            values = [self.code]
            for td in tr.find_all('td'):
                values.append(td.text)
            sel = ('code', self.code, 'report_date', values[1])
            self.storage(values, sel)
            amount += 1
        printf("jj_info_profit storage,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_cost(Spider):
    def start(self):
        self.initialization("info_cost", "jj_info_cost")
        source = self._webdriver.get(self.url)
        table = source.find('div', class_='txt_cont').find('table', class_='w782 comm income').find('tbody').find_all(
            'tr')
        amount = 0
        for tr in table:
            values = [self.code]
            for td in tr.find_all('td'):
                values.append(td.text)
            sel = ('code', self.code, 'report_date', values[1])
            self.storage(values, sel)
            amount += 1
        printf("jj_info_cost storage,CODE:%s,AMOUNT:%s" % (self.code, amount))


class Spider_purchase_info(Spider):
    def start(self):
        self.initialization("info_purchase_info", "jj_info_purchase_info")
        source = self._webdriver.get(self.url)
        boxes = source.find('div', class_='txt_in').find_all('div', class_='box')
        values = [self.code]
        for box in boxes[:4]:
            for table in box.find_all('tbody'):
                for tr in table.find_all('tr'):
                    tds = tr.find_all('td')
                    for i in range(int(len(tds) / 2)):
                        text = tds[2 * i + 1].text
                        if text:
                            values.append(text)
        for box in boxes[4:-1]:
            string = ''
            for table in box.find_all('tbody'):
                for tr in table.find_all('tr'):
                    for td in tr.find_all('td'):
                        string += td.text
                    string += ";"
            values.append(string)
        sel = ('code', self.code)
        self.storage(values, sel)
        printf("jj_info_purchase_info storage,CODE:%s" % self.code)


class Spider_swich_info(Spider):
    def start(self):
        self.initialization("info_swich_info", "jj_info_swich_info")
        self.url = "http://fund.eastmoney.com/f10/jjzh_166006.html"
        source = self._webdriver.get(self.url)
        boxes = source.find('div', class_='txt_in').find_all('div', class_='box')
        amount = 0
        for box in boxes[1:]:
            for tr in box.find('tbody').find_all('tr'):
                values = [self.code, box.find('h4').text]
                tds = tr.find_all('td')
                values.append(tds[0].text)
                values.append(tds[1].text)
                sel = ('code', self.code, 'target_code', tds[0].text)
                self.storage(values, sel)
                amount += 1
        printf("jj_info_swich_info storage,CODE:%s,AMOUNT:%s" % (self.code, amount))


Spider_List = [subclass for subclass in Spider.__subclasses__()]
Spider_List.remove(Spider_basic_list)
# Spider_List.remove(Spider_basic_info)

if __name__ == "__main__":
    tmp = Spider_property_pz()
    tmp.start()
    tmp.close()